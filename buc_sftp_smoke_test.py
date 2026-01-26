#!/usr/bin/env python3
"""
BUC SFTP smoke test (standalone).

What it does:
1) Load bucdaq.yml (or a path you pass via --config)
2) Create a small temporary test file
3) Upload it to <remote_path>/integration-tests/
4) Verify it exists, size matches, and contents match
5) Remove it again from the remote server

Usage:
  python scripts/buc_sftp_smoketest.py
  python scripts/buc_sftp_smoketest.py --config /path/to/bucdaq.yml
  SFTP_KEY_PASSPHRASE=... python scripts/buc_sftp_smoketest.py

Exit codes:
  0 success
  1 failure
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Any

import paramiko


def load_config(config_path: Path) -> dict[str, Any]:
    """
    Load YAML config.

    Prefer nrbdaq.utils.utils.load_config if available (keeps behavior consistent with your app),
    otherwise fallback to PyYAML.
    """
    try:
        from nrbdaq.utils.utils import load_config as _load_config  # type: ignore
    except Exception:
        _load_config = None

    if _load_config is not None:
        cfg = _load_config(config_file=str(config_path), as_dict=True)
        if not isinstance(cfg, dict):
            raise TypeError("load_config() did not return a dict")
        return cfg  # type: ignore[return-value]

    try:
        import yaml  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "PyYAML not available and nrbdaq.utils.utils.load_config not importable"
        ) from e

    with config_path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        raise TypeError("YAML config did not parse to a dict")
    return cfg  # type: ignore[return-value]


def load_private_key(key_path: Path) -> paramiko.PKey:
    """
    Load a private key file (supports Ed25519/RSA/ECDSA), optionally using passphrase.

    If your key is encrypted, set:
        SFTP_KEY_PASSPHRASE="..."
    """
    passphrase = os.environ.get("SFTP_KEY_PASSPHRASE")
    loaders = (
        paramiko.Ed25519Key,
        paramiko.RSAKey,
        paramiko.ECDSAKey,
    )

    last_err: Exception | None = None
    for cls in loaders:
        try:
            return cls.from_private_key_file(str(key_path), password=passphrase)
        except Exception as e:
            last_err = e

    raise RuntimeError(
        f"Could not load private key from {key_path}. Last error: {last_err}"
    ) from last_err


def posix_norm(path: str) -> str:
    """Normalize remote POSIX-ish paths like './incoming/buc' -> 'incoming/buc'."""
    p = path.replace("\\", "/").strip()
    while p.startswith("./"):
        p = p[2:]
    return p.strip("/")


def ensure_remote_dir(sftp: paramiko.SFTPClient, remote_dir: str) -> None:
    """
    Ensure remote_dir exists by creating missing path elements.

    Works for both relative (to login dir) and absolute-ish paths.
    """
    remote_dir = posix_norm(remote_dir)
    if not remote_dir:
        return

    parts = [p for p in remote_dir.split("/") if p]
    current = "."

    for part in parts:
        nxt = f"{current}/{part}" if current not in (".", "") else part
        try:
            sftp.stat(nxt)
        except FileNotFoundError:
            sftp.mkdir(nxt)
        current = nxt


def remote_exists(sftp: paramiko.SFTPClient, remote_path: str) -> bool:
    try:
        sftp.stat(remote_path)
        return True
    except FileNotFoundError:
        return False


def main() -> int:
    ap = argparse.ArgumentParser(description="BUC SFTP smoke test (upload/verify/remove).")
    ap.add_argument(
        "--config",
        type=str,
        default=os.environ.get("BUCDAQ_CONFIG", "bucdaq.yml"),
        help="Path to bucdaq.yml (default: BUCDAQ_CONFIG env var or ./bucdaq.yml).",
    )
    ap.add_argument(
        "--timeout",
        type=int,
        default=int(os.environ.get("BUC_SFTP_TIMEOUT", "15")),
        help="SSH connect/auth timeout in seconds (default: 15 or BUC_SFTP_TIMEOUT).",
    )
    args = ap.parse_args()

    config_path = Path(args.config).expanduser()
    if not config_path.exists():
        print(f"ERROR: config file not found: {config_path}", file=sys.stderr)
        return 1

    cfg = load_config(config_path)
    if "sftp" not in cfg:
        print("ERROR: config has no 'sftp' section.", file=sys.stderr)
        return 1

    sftp_cfg = cfg["sftp"]
    host = str(sftp_cfg["host"])
    usr = str(sftp_cfg["usr"])
    port = int(sftp_cfg.get("port", 22))
    key_path = Path(str(sftp_cfg["key"])).expanduser()
    remote_root = str(sftp_cfg["remote_path"])

    if not key_path.exists():
        print(f"ERROR: private key not found: {key_path}", file=sys.stderr)
        return 1

    token = uuid.uuid4().hex
    payload = f"BUC SFTP integration smoke test\nid={token}\n".encode("utf-8")

    # Create a temp file
    with tempfile.TemporaryDirectory(prefix="buc_sftp_smoketest_") as td:
        local_file = Path(td) / f"buc_sftp_integration_{token}.txt"
        local_file.write_bytes(payload)

        # Remote location
        remote_dir = f"{remote_root.rstrip('/')}/integration-tests"
        remote_dir_norm = posix_norm(remote_dir)
        remote_file = f"{remote_dir_norm}/{local_file.name}"

        key = load_private_key(key_path)

        ssh: paramiko.SSHClient | None = None
        sftp: paramiko.SFTPClient | None = None

        print(f"[info] host={host} port={port} user={usr}")
        print(f"[info] key={key_path}")
        print(f"[info] remote_dir={remote_dir_norm}")
        print(f"[info] remote_file={remote_file}")

        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                hostname=host,
                port=port,
                username=usr,
                pkey=key,
                timeout=args.timeout,
                banner_timeout=args.timeout,
                auth_timeout=args.timeout,
                look_for_keys=False,
                allow_agent=False,
            )

            sftp = ssh.open_sftp()

            # Ensure remote directory exists
            ensure_remote_dir(sftp, remote_dir_norm)

            # Upload
            print("[step] upload")
            sftp.put(str(local_file), remote_file, confirm=True)

            # Verify exists + size
            print("[step] verify exists + size")
            if not remote_exists(sftp, remote_file):
                raise RuntimeError(f"Remote file not found after upload: {remote_file}")

            st = sftp.stat(remote_file)
            if st.st_size != len(payload):
                raise RuntimeError(f"Remote size {st.st_size} != local size {len(payload)}")

            # Verify contents
            print("[step] verify content")
            with sftp.open(remote_file, "rb") as fh:
                got = fh.read()
            if got != payload:
                raise RuntimeError("Remote payload differs from local payload")

            print("[step] remove remote file")
            sftp.remove(remote_file)

            # Confirm deletion
            if remote_exists(sftp, remote_file):
                raise RuntimeError("Remote file still exists after removal")

            print("SUCCESS")
            return 0

        except Exception as e:
            print(f"FAILED: {e}", file=sys.stderr)
            # best-effort cleanup
            try:
                if sftp is not None and remote_exists(sftp, remote_file):
                    sftp.remove(remote_file)
                    print("[cleanup] removed remote test file")
            except Exception as ce:
                print(f"[cleanup] could not remove remote test file: {ce}", file=sys.stderr)
            return 1

        finally:
            try:
                if sftp is not None:
                    sftp.close()
            except Exception:
                pass
            try:
                if ssh is not None:
                    ssh.close()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
