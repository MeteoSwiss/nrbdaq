# fidas.py — drop-in replacement
from __future__ import annotations

import socket
import polars as pl
import datetime
import schedule
import time
import re
from pathlib import Path
from typing import Any

from nrbdaq.utils.utils import setup_logging


class FIDAS:
    """
    FIDAS UDP data collector with minute aggregation.

    Public API (unchanged):
      - __init__(config: dict, name: str='fidas')
      - __enter__ / __exit__
      - connect_udp()
      - setup_schedules()
      - run()
      - collect_raw_record()
      - compute_minute_median()
      - save_hourly(stage: bool = True)
      - ensure_output_path(dt: datetime)

    The wire payload is a sequence of frames like:
        6111<sendVal 0=1.0000;1=0.0000;...;74=0.0000>346111<sendVal 110=...>...

    We normalize each frame to: "{id}<sendVal ...>{checksum}" where id/checksum
    are kept as strings (hex or decimal).
    """

    # ---- INIT / CONTEXT -----------------------------------------------------

    def __init__(
        self,
        config: dict,
        name: str = "fidas",
    ):
        self.name = name

        # configure logging
        logfile = Path(config["root"]).expanduser() / config["logging"]["file"]
        self.logger = setup_logging(
            file=logfile,
            level_console=config["logging"]["level_console"],
            level_file=config["logging"]["level_file"],
        )
        self.logger.info("Initialize FIDAS", extra={"to_logfile": True})

        # data paths
        self.data_dir = Path(config["root"]).expanduser() / config["data"] / config[name]["data_path"]
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.staging_path = Path(config["root"]).expanduser() / config["staging"] / config[name]["staging_path"]
        self.staging_path.mkdir(parents=True, exist_ok=True)

        self.remote_path = config[name]["remote_path"]
        # Note: remote_path may be meant for SFTP; we keep prior behavior.
        Path(self.remote_path).mkdir(parents=True, exist_ok=True)

        # scheduling / socket config
        self.fetch_interval_seconds = int(config[name]["fetch_interval_seconds"])
        self.reporting_interval = config[name]["reporting_interval"]
        self.local_ip = config[name]["socket"]["host"]
        self.local_port = config[name]["socket"]["port"]
        self.buffer_size = int(config[name]["socket"]["buffer_size"])

        # runtime state
        self.sock: socket.socket | None = None
        self.buffer: str = ""  # carry-over buffer between UDP recv calls
        self._warmup_left: int = 2  # skip first couple of fragments after bind

        # accumulation + minute medians
        self.raw_records: list[dict[str, Any]] = []
        self.df_minute: pl.DataFrame = pl.DataFrame()
        self.current_hour = (
            datetime.datetime.now(datetime.timezone.utc).replace(minute=0, second=0, microsecond=0)
        )

    def __enter__(self):
        try:
            self.connect_udp()
            self.setup_schedules()
            return self
        except Exception as err:
            self.logger.error(f"[FIDAS.__enter__] {err} {self.local_ip}:{self.local_port}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.sock:
                try:
                    self.sock.close()
                except Exception:
                    pass
            if not self.df_minute.is_empty():
                # ensure we persist what we have
                self.save_hourly(stage=True)
        except Exception as err:
            self.logger.error(f"[FIDAS.__exit__] {err} {self.local_ip}:{self.local_port}")
        self.logger.info("[FIDAS.__exit__] Goodbye!", extra={"to_logfile": True})

    # ---- SOCKET / IO --------------------------------------------------------

    def connect_udp(self):
        """Open UDP socket and bind; use restart-friendly options."""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # restart-friendly & burst-tolerant
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass
            # give headroom for bursts (Linux may double internally)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
            self.sock.bind((self.local_ip, self.local_port))
            # keep original timing model (blocking with timeout)
            self.sock.settimeout(self.fetch_interval_seconds)
            self.logger.info(f"[FIDAS.__enter__] Listening on {self.local_ip}:{self.local_port}")
            return
        except Exception as err:
            self.logger.error(f"[.connect_udp] {err}")

    def receive_udp_record(self) -> str:
        """
        Read from UDP until we can return exactly one normalized frame:
            "{id}<sendVal ...>{checksum}"
        Keeps remainder in self.buffer for next call.

        Returns empty string on timeout.
        """
        if self.sock is None:
            return str()

        deadline = time.time() + max(0.5, float(self.fetch_interval_seconds))
        while time.time() < deadline:
            try:
                data, _ = self.sock.recvfrom(self.buffer_size)
                self.buffer += data.decode("ascii", errors="ignore")
            except socket.timeout:
                # If we already have a complete frame in buffer, try to parse it now.
                pass

            # Trim runaway buffer
            if len(self.buffer) > 262_144:
                self.buffer = self.buffer[-131_072:]

            # Look for the first '<sendVal '
            sof = self.buffer.find("<sendVal ")
            if sof == -1:
                # no start yet; keep only last few KB
                if len(self.buffer) > 4096:
                    self.buffer = self.buffer[-4096:]
                continue

            # We have a start; ensure we also have the end '>' after that.
            eof = self.buffer.find(">", sof)
            if eof == -1:
                # not a full frame yet
                continue

            # Extract ID (hex/dec) immediately before the '<sendVal '
            pre = self.buffer[:sof]
            m_id = re.search(r"([0-9A-Fa-f]+)\s*$", pre)
            id_part = m_id.group(1) if m_id else ""

            # The payload (without leading '<' and trailing '>'):
            payload = self.buffer[sof + 1 : eof]  # "sendVal 0=...;..."

            # Extract checksum characters immediately after '>'
            m_cs = re.match(r"([0-9A-Fa-f]+)", self.buffer[eof + 1 :])
            checksum = m_cs.group(1) if m_cs else ""

            # Consume the used portion from the buffer
            consume_upto = eof + 1 + (len(checksum) if checksum else 0)
            self.buffer = self.buffer[consume_upto:]

            # Skip first couple frames after bind to avoid mid-stream tails
            if self._warmup_left > 0:
                self._warmup_left -= 1
                continue

            # Return a *single* normalized record
            return f"{id_part}<{payload}>{checksum}"

        # No complete frame in this call
        return str()

    # ---- PARSING / AGG ------------------------------------------------------

    def parse_record(self, record: str) -> "dict[str, Any]":
        """
        Parse a normalized record: "{id}<sendVal k=v;...>{checksum}"
        - id/checksum kept as strings (could be hex).
        - values converted to float; 'NaN' -> None.
        Returns {} on parse error.
        """
        self.logger.debug("[.parse_record] entering function")
        try:
            id_part, rest = record.split("<", 1)
            data_part, checksum = rest.split(">", 1)

            parsed: dict[str, Any] = {
                "id": id_part.strip(),         # keep as string (hex allowed)
                "checksum": checksum.strip(),  # raw
            }

            # remove 'sendVal' prefix if present
            if data_part.startswith("sendVal"):
                data_part = data_part[len("sendVal") :].strip()

            for pair in data_part.split(";"):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    key = k.strip()  # keep numeric index as string ("60", "110", ...)
                    t = v.strip()
                    if t.lower() == "nan":
                        val = None
                    else:
                        try:
                            val = float(t)
                        except ValueError:
                            val = None
                    parsed[key] = val

            return parsed
        except Exception as e:
            self.logger.error(f"[.parse_record] failed to parse record: {e}")
            return {}

    def collect_raw_record(self):
        """Fetch one frame (if available), parse, and append to raw buffer."""
        self.logger.debug("[.collect_raw_record] entering ...")
        record = self.receive_udp_record()
        self.logger.debug(f"[.collect_raw_record] {record[:100]}")
        if record:
            parsed = self.parse_record(record)
            if parsed:
                self.raw_records.append(parsed)
                self.logger.debug("[.collect_raw_record] raw_record appended")
        else:
            self.logger.debug("[.collect_raw_record] no complete frame available this tick")

    def compute_minute_median(self):
        """
        Aggregate whatever is in raw_records into one minute-median row and
        append it to df_minute. Clears raw_records afterwards.
        """
        self.logger.debug("[.compute_minute_median] entering ...")
        if not self.raw_records:
            self.logger.debug("[.compute_minute_median] self.raw_records is empty.")
            return

        # Validate structure
        if not all(isinstance(row, dict) for row in self.raw_records):
            self.logger.error(f"[.compute_minute_median] Invalid format in raw_records: {self.raw_records}")
            return

        df = pl.DataFrame(self.raw_records)

        # Select only float columns, excluding id/checksum
        value_cols = [
            col
            for col, dtype in df.schema.items()
            if col not in {"id", "checksum"} and dtype in {pl.Float64, pl.Float32}
        ]

        if not value_cols:
            self.logger.debug("[.compute_minute_median] No numeric value columns to aggregate.")
            self.raw_records.clear()
            return

        median_row = df.select([pl.col(c).median().alias(c) for c in value_cols])

        now = datetime.datetime.now(datetime.timezone.utc)
        median_row = median_row.with_columns(
            [
                pl.lit("median").alias("id"),
                pl.lit("").alias("checksum"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("dtm"),
            ]
        )

        # Ensure any columns present in df but absent in median also exist (as None)
        for col in df.columns:
            if col not in median_row.columns:
                median_row = median_row.with_columns(pl.lit(None).alias(col))

        # Consistent column order
        median_row = median_row.select(sorted(median_row.columns))

        # Append and clear buffer
        self.df_minute = pl.concat([self.df_minute, median_row], how="diagonal")
        self.raw_records.clear()

        # Optional: quick log of key fields (adjust as you like)
        _map = {
            "60": "Cn [P/cm³]",
            "61": "PM1 [mg/m³]",
            "62": "PM2.5 [mg/m³]",
            "63": "PM4 [mg/m³]",
            "64": "PM10 [mg/m³]",
            "65": "PMtotal [mg/m³]",
        }
        values = {lbl: median_row.item(0, col) if col in median_row.columns else None for col, lbl in _map.items()}
        self.logger.info(f"[.compute_minute_median] row added: {values}")

    # ---- PERSISTENCE / SCHEDULE ---------------------------------------------

    def save_hourly(self, stage: bool = True):
        """At the top of a new hour, write previous hour's df_minute to disk (and stage)."""
        self.logger.debug("[.save_hourly] entering ...")
        now = datetime.datetime.now(datetime.timezone.utc)
        if now.hour != self.current_hour.hour:
            if not self.df_minute.is_empty():
                out_path = self.ensure_output_path(self.current_hour)
                if out_path.exists():
                    existing = pl.read_parquet(out_path)
                    self.df_minute = pl.concat([existing, self.df_minute], how="diagonal").unique()
                self.df_minute.write_parquet(out_path)
                if stage:
                    self.staging_path.mkdir(parents=True, exist_ok=True)
                    staging_path = self.staging_path / out_path.name
                    self.df_minute.write_parquet(staging_path)
                self.logger.debug(
                    f"[.save_hourly] hourly file saved to {out_path} and staged to {staging_path}"
                )
            # reset for the new hour
            self.df_minute = pl.DataFrame()
            self.current_hour = now.replace(minute=0, second=0, microsecond=0)

    def ensure_output_path(self, dt: datetime.datetime) -> Path:
        folder = self.data_dir / f"{dt.year:04d}" / f"{dt.month:02d}" / f"{dt.day:02d}"
        folder.mkdir(parents=True, exist_ok=True)
        filename = f"{self.name}-{dt.year:04d}{dt.month:02d}{dt.day:02d}{dt.hour:02d}.parquet"
        return folder / filename

    def setup_schedules(self):
        schedule.every(self.fetch_interval_seconds).seconds.do(self.collect_raw_record)
        schedule.every(1).minutes.do(self.compute_minute_median)
        schedule.every(1).hours.do(self.save_hourly)
        return

    def run(self):
        try:
            self.logger.info(schedule.get_jobs())
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            print("Stopping FIDAS...")
            self.save_hourly()  # Save any remaining data on exit
