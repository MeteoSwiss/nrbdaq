import schedule
import time
from pathlib import Path
from typing import Any

from nrbdaq.instr.thermo import Thermo49i
from nrbdaq.utils.sftp import SFTPClient
from nrbdaq.utils.utils import load_config, seconds_to_next_n_minutes, setup_logging


def main():
    # load configuration (as_dict=True => Pylance infers dict[str, Any])
    config: dict[str, Any] = load_config(config_file="bucdaq.yml", as_dict=True)

    # setup logging
    root = Path(str(config["root"])).expanduser()
    logfile = root / str(config["logging"]["file"])
    logger = setup_logging(file=str(logfile))
    logger.info("== Start BUCDAQ =============", extra={"to_logfile": True})

    # setup sftp client
    sftp = SFTPClient(config=config)
    logger.debug(f"sftp.remote_path: {sftp.remote_path}")

    # setup Thermo 49i data acquisition and data transfer
    thermo49i = Thermo49i(config=config)
    thermo49i.setup_schedules()

    # remote paths are POSIX-like; keep them as strings for the SFTP layer
    remote_path = f"{sftp.remote_path.rstrip('/')}/{thermo49i.remote_path.lstrip('/')}"
    sftp.setup_transfer_schedules(
        local_path=thermo49i.staging_path,
        remote_path=remote_path,
        interval=thermo49i.reporting_interval,
    )

    # list all jobs
    logger.info(schedule.get_jobs(), extra={"to_logfile": True})

    # align start with a multiple-of-minute timestamp
    seconds_left = seconds_to_next_n_minutes(1)
    while seconds_left > 0:
        # logging.Logger.info() does not support `end=...` (that's for print()).
        print(f"Time remaining: {seconds_left:>0.1f} s", end="\r", flush=True)
        dt = 1
        time.sleep(dt)
        seconds_left -= dt

    logger.info("Beginning data acquisition and file transfer ...")

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping data acquisition ...")


if __name__ == "__main__":
    main()
