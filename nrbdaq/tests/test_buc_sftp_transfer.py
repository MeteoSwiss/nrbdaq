# tests/integration/test_buc_sftp_transfer.py

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any

import pytest

import paramiko


pytestmark = pytest.mark.integration


def _load_config(config_path: Path) -> dict[str, Any]:
    """
    Load YAML config.

    Prefer nrbdaq.utils.utils.load_config if available (keeps behavior consistent with your app),
    otherwise fallback to PyYAML.
    """
    try:
        from nrbdaq.utils.utils import load_config  # type: ignore
    except Exception:
        load_config = None

    if load_config is not None:
        cfg = load_config(config_file=str(config_path), as_dict=True)
        if not isinstance(cfg, dict):
            raise TypeError("load_config() did not return a dict")
        return cfg  # type: ignore[return-value]

    try:
        import yaml  # type: ignore
    except Exception as e:
        raise RuntimeError("PyYAML not available and nrbdaq.utils.utils.load_config not importable") from e

    with config_path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        raise TypeError("YAML config did not parse to a dict")
    return cfg  # type: ignore[return-value]


def _load_private_key(key_path: Path) -> paramiko.PKey:
    """
    Load a private key file (supports Ed25519/RSA/ECDSA/DSA), optionally using passphrase.

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

    raise RuntimeError(f"Could not load private key from {key_path}. Last error: {last_err}") from last_err


def _posix_norm(path: str) -> str:
    """Normalize remote POSIX-ish paths like './incoming/buc' -> 'incoming/buc'."""
    p = path.replace("\\", "/").strip()
    while p.startswith("./"):
        p = p[2:]
    return p.strip("/")


def _ensure_remote_dir(sftp: paramiko.SFTPClient, remote_dir: str) -> None:
    """
    Ensure remote_dir exists by creating missing path elements.

    Works for both relative (to login dir) and absolute-ish paths.
    """
    remote_dir = _posix_norm(remote_dir)
    if not remote_dir:
        return

    parts = [p for p in remote_dir.split("/") if p]
    current = "."

    for part in parts:
        nxt = f"{current}/{part}" if current not in (".", "") else part
        try:
            sftp.stat(nxt)
        except FileNotFoundError:
            # create and continue
            sftp.mkdir(nxt)
        current = nxt


def _remote_exists(sftp: paramiko.SFTPClient, remote_path: str) -> bool:
    try:
        sftp.stat(remote_path)
        return True
    except FileNotFoundError:
        return False


@pytest.mark.skipif(
    os.environ.get("BUC_SFTP_INTEGRATION", "0") not in ("1", "true", "TRUE", "yes", "YES"),
    reason="Set BUC_SFTP_INTEGRATION=1 to run this SFTP integration test.",
)
def test_buc_sftp_put_verify_remove(tmp_path: Path) -> None:
    """
    Integration test: upload -> verify -> delete a file on the BUC SFTP server.

    Env vars:
      - BUC_SFTP_INTEGRATION=1     enables this test
      - BUCDAQ_CONFIG=/path/to/bucdaq.yml   optional (default: ./bucdaq.yml)
      - SFTP_KEY_PASSPHRASE=...    optional if your private key is encrypted
    """
    config_path = Path(os.environ.get("BUCDAQ_CONFIG", "bucdaq.yml")).expanduser()
    if not config_path.exists():
        pytest.skip(f"Config file not found: {config_path}")

    cfg = _load_config(config_path)

    if "sftp" not in cfg:
        pytest.skip("No 'sftp' section in config; nothing to test.")

    sftp_cfg = cfg["sftp"]
    host = str(sftp_cfg["host"])
    usr = str(sftp_cfg["usr"])
    key_path = Path(str(sftp_cfg["key"])).expanduser()
    remote_root = str(sftp_cfg["remote_path"])

    if not key_path.exists():
        pytest.skip(f"Private key not found: {key_path}")

    # Create a small local test file
    token = uuid.uuid4().hex
    local_file = tmp_path / f"buc_sftp_integration_{token}.txt"
    payload = f"BUC SFTP integration test\nid={token}\n".encode("utf-8")
    local_file.write_bytes(payload)

    # Remote location (keep under an integration-tests subfolder)
    remote_dir = f"{remote_root.rstrip('/')}/integration-tests"
    # Normalize to something paramiko SFTP likes
    remote_dir_norm = _posix_norm(remote_dir)
    remote_file = f"{remote_dir_norm}/{local_file.name}"

    key = _load_private_key(key_path)

    ssh: paramiko.SSHClient | None = None
    sftp: paramiko.SFTPClient | None = None

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=host, username=usr, pkey=key)

        sftp = ssh.open_sftp()

        # Ensure remote directory exists
        _ensure_remote_dir(sftp, remote_dir)

        # Upload
        sftp.put(str(local_file), remote_file, confirm=True)

        # Verify: exists + size
        assert _remote_exists(sftp, remote_file), f"Remote file not found after upload: {remote_file}"
        st = sftp.stat(remote_file)
        assert st.st_size == len(payload), f"Remote size {st.st_size} != local size {len(payload)}"

        # Verify: contents
        with sftp.open(remote_file, "rb") as fh:
            got = fh.read()
        assert got == payload, "Remote payload differs from local payload"

    finally:
        # Cleanup remote file (best effort, but we *want* it removed when permissions allow)
        try:
            if sftp is not None and _remote_exists(sftp, remote_file):
                sftp.remove(remote_file)
        except Exception as e:
            # Make cleanup failures visible (permissions / policy issues)
            pytest.fail(f"Uploaded test file but could not remove it: {remote_file}. Error: {e}")

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

    # Confirm deletion (new connection not necessary; reusing sftp in finally is enough, but keep explicit)
    # If we reach here, remove succeeded.

# Use 'BUC_SFTP_INTEGRATION=1 pytest nrbdaq/tests/test_buc_sftp_transfer.py -m integration' to run this test file.