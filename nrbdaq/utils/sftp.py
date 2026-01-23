#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Manage file transfer.

Supported:
- SFTP transfer (key-based) via Paramiko (existing functionality)
- FTP transfer (user/password) via ftplib (new)

Security note:
Plain FTP is unencrypted (credentials and data are transferred in clear text).
Use only when required by the destination server.

@author: joerg.klausen@meteoswiss.ch
"""
import ftplib
import logging
import os
import re
from pathlib import Path

import paramiko
import schedule


def _normalize_path(path) -> str:
    """
    Normalize a path-like input to a forward-slash string.

    Accepts:
    - str
    - any os.PathLike (Path, PosixPath, WindowsPath, PurePath, PurePosixPath, ...)

    Returns:
        str: path string with forward slashes
    """
    if path is None:
        return str()
    if isinstance(path, os.PathLike):
        path = os.fspath(path)
    if not isinstance(path, str):
        raise TypeError(f"Unsupported path type: {type(path)}")
    return path.replace("\\", "/")


def _resolve_under_root(root: str, maybe_relative_path: str) -> str:
    """
    Resolve a path that may be relative to the configured root.

    - Expands ~
    - If absolute after expansion: returns as-is
    - Else: joins with expanded root
    """
    p = os.path.expanduser(str(maybe_relative_path))
    if os.path.isabs(p):
        return p
    return os.path.join(os.path.expanduser(str(root)), p)


class SFTPClient:
    """
    SFTP based file handling, optionally using SOCKS5 proxy.

    Available methods include
    - is_alive():
    - list_local_files():
    - remote_item_exists():
    - list_remote_items():
    - setup_remote_folders():
    - put_file():
    - remove_remote_item():
    - transfer_files(): transfer files, optionally removing files from source
    """

    def __init__(self, config: dict):
        """
        Initialize the SFTPClient class with parameters from a configuration file.

        Expected config keys:
            config['sftp']['host']
            config['sftp']['usr']
            config['sftp']['key']          (path to private key)
            config['sftp']['remote_path']  (remote destination root)

        Local source is derived from:
            config['root'] + config['staging']
        """
        try:
            # configure logging
            _logger = f"{os.path.basename(config['logging']['file'])}".split(".")[0]
            self.logger = logging.getLogger(f"{_logger}.{__name__}")
            self.schedule_logger = logging.getLogger(f"{_logger}.schedule")
            self.schedule_logger.setLevel(level=logging.DEBUG)
            self.logger.info("Initialize SFTPClient")

            # sftp connection settings
            self.host = config["sftp"]["host"]
            self.usr = config["sftp"]["usr"]
            self.key = paramiko.RSAKey.from_private_key_file(
                os.path.expanduser(config["sftp"]["key"])
            )

            # configure local source
            self.local_path = os.path.join(os.path.expanduser(config["root"]), config["staging"])
            self.logger.debug(f"__init__: {self.local_path}")

            # configure remote destination
            self.remote_path = config["sftp"]["remote_path"]
            self.logger.debug(f"__init__: {self.remote_path}")

        except Exception as err:
            self.logger.error(err)

    def is_alive(self) -> bool:
        """Test ssh connection to sftp server."""
        try:
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)

                with ssh.open_sftp() as sftp:
                    sftp.close()
            return True
        except Exception as err:
            self.logger.error(err)
            return False

    def list_local_files(self, local_path: str = str()) -> list:
        """Establish list of local files.

        Args:
            local_path (str, optional): Absolute path to directory containing folders and files.

        Returns:
            list: absolute paths of local files
        """
        if local_path is None:
            local_path = self.local_path

        try:
            files = []
            for root, _, filenames in os.walk(local_path):
                for file in filenames:
                    files.append(os.path.join(root, file))
            return files

        except Exception as err:
            self.logger.error(err)
            return list()

    def remote_item_exists(self, remote_path: str) -> bool:
        """Check on remote server if an item exists. Assume this indicates successful transfer."""
        try:
            remote_path = remote_path.replace("\\", "/").rstrip("/")
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)
                with ssh.open_sftp() as sftp:
                    try:
                        sftp.stat(remote_path)
                        return True
                    except FileNotFoundError:
                        return False
        except Exception as err:
            self.logger.error(err)
            return False

    def list_remote_items(self, remote_path: str = ".") -> list:
        try:
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)
                with ssh.open_sftp() as sftp:
                    return sftp.listdir(remote_path)

        except Exception as err:
            self.logger.error(err)
            return list()

    def setup_remote_folders(self, local_path: str = str(), remote_path: str = str()) -> None:
        """
        Determine directory structure under local_path and replicate on remote host.
        """
        try:
            if local_path is None:
                local_path = self.local_path

            # sanitize local_path
            local_path = re.sub(r"(/?\.?\\){1,2}", "/", local_path)

            if remote_path is str():
                remote_path = self.remote_path

            # sanitize remote_path
            remote_path = re.sub(r"(\\){1,2}", "/", remote_path)

            self.logger.info(
                f"setup_remote_folders (local_path: {local_path}, remote_path: {remote_path})"
            )

            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)
                with ssh.open_sftp() as sftp:
                    # determine local directory structure, establish same structure on remote host
                    for root, dirs, files in os.walk(local_path):
                        root = re.sub(r"(/?\.?\\){1,2}", "/", root).replace(local_path, remote_path)
                        self.logger.debug(f"root: {root}")
                        try:
                            sftp.mkdir(root, mode=16877)
                        except OSError as err:
                            self.logger.error(
                                f"Could not create '{root}', error: {err}. Maybe path exists already?"
                            )
                            pass
                    sftp.close()

        except Exception as err:
            self.logger.error(err)

    def put_file(self, local_path: str, remote_path: str):
        """Send a file to a remote host using SFTP and SSH."""
        try:
            if os.path.exists(local_path):
                # remove the file name from remote_path in case it was appended, then add the file name
                remote_path = os.path.join(os.path.dirname(remote_path), os.path.basename(local_path)).replace(
                    "\\", "/"
                )
                with paramiko.SSHClient() as ssh:
                    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)
                    with ssh.open_sftp() as sftp:
                        attr = sftp.put(localpath=local_path, remotepath=remote_path, confirm=True)
                        sftp.close()
                    self.logger.info(f"put_file {local_path} > {remote_path}")
                return attr
            else:
                raise ValueError(f"local_path {local_path} does not exist.")
        except Exception as err:
            self.logger.error(err)

    def remove_remote_item(self, remote_path: str) -> None:
        """
        Remove a file or prune (the last part of remote_path, not iterative) an (empty) directory.
        """
        try:
            remote_path = remote_path.replace("\\", "/")
            if self.remote_item_exists(remote_path):
                with paramiko.SSHClient() as ssh:
                    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)
                    with ssh.open_sftp() as sftp:
                        try:
                            if sftp.listdir(remote_path):
                                self.logger.warning(
                                    "Cannot remove non-empty directory. Provide full path to file to remove it, or empty the directory first."
                                )
                                return
                            else:
                                sftp.rmdir(remote_path)
                        except Exception:
                            try:
                                sftp.remove(remote_path)
                            except Exception as err:
                                self.logger.error(err)
                        self.logger.info(f"remove_remote_item {remote_path}")
                        sftp.close()

            else:
                raise ValueError("remove_remote_item: remote_path does not exist.")
        except Exception as err:
            self.logger.error(f"remove_remote_item: {err}")

    def setup_remote_path(self, remote_path: str) -> str:
        """Create (and navigate to the leaf of) a remote path (SFTP)."""
        try:
            remote_path = remote_path.replace("\\", "/").replace("./", "")
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)
                with ssh.open_sftp() as sftp:
                    try:
                        sftp.chdir(remote_path)
                    except IOError:
                        parts = remote_path.split("/")
                        current_path = "."
                        for part in parts:
                            if part:
                                current_path = f"{current_path}/{part}"
                            try:
                                sftp.chdir(current_path)
                            except IOError:
                                sftp.mkdir(part)
                                sftp.chdir(part)
                                self.logger.debug(f"setup_remote_path: created {part}")
                    cwd = sftp.getcwd()
                    self.logger.debug(f"setup_remote_path: switched to {cwd}")
                    if cwd is None:
                        cwd = str()
            return cwd
        except Exception as err:
            self.logger.error(f"setup_remote_path: {err}")
            return str()

    def normalize_path(self, path) -> str:
        """
        Normalize a path to a string with forward slashes, regardless of input type.
        Accepts str and any os.PathLike (Path, PurePath, etc.).
        """
        try:
            return _normalize_path(path)
        except Exception as err:
            self.logger.error(f"[normalize_path] {err}")
            return str()

    def transfer_files(self, local_path: str = str(), remote_path: str = str(), remove_on_success: bool = True) -> None:
        """Transfer (move) all files from local_path and sub-folders to remote_path."""
        try:
            self.transfered = []
            if not local_path:
                local_path = self.local_path
            if not remote_path:
                remote_path = self.remote_path

            # sanitize paths
            local_path = self.normalize_path(local_path)
            remote_path = self.normalize_path(remote_path)
            self.logger.info(f"{local_path} > {remote_path}", extra={"to_logfile": True})

            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)
                with ssh.open_sftp() as sftp:
                    top = local_path
                    for root, _, files in os.walk(top=top):
                        for file in files:
                            local_file = os.path.join(root, file).replace("\\", "/").rstrip("/")
                            self.logger.info(f"{local_file}", extra={"to_logfile": True})

                            parts = root.replace("\\", "/").replace(local_path, "").strip("/")
                            remote_file = f"{remote_path}/{parts}/{file}"
                            self.logger.info(f"{remote_file}", extra={"to_logfile": True})

                            _ = self.setup_remote_path(f"{remote_path}/{parts}")

                            attr = sftp.put(localpath=local_file, remotepath=remote_file, confirm=True)
                            self.logger.debug(f"put {local_file} > {remote_file}")
                            self.transfered.append(file)

                            if remove_on_success:
                                local_size = os.stat(local_file).st_size
                                remote_size = attr.st_size
                                if remote_size == local_size:
                                    os.remove(local_file)
                                else:
                                    self.logger.warning(
                                        f"local file size: {local_size}, remote file: {remote_size} differ. Did not remove {local_file}."
                                    )
                return

        except Exception as err:
            self.logger.error(f"transfer_files: {local_path} > {remote_path}: {err}")

    def setup_transfer_schedules(self, local_path: str, remote_path: str, remove_on_success: bool = True, interval: int = 60):
        """
        Setup scheduled transfer jobs.

        interval:
            - 10 minutes, or
            - multiple of 60 minutes, up to 1440 (daily)
        """
        try:
            local_path = self.normalize_path(local_path)
            remote_path = self.normalize_path(remote_path)

            if interval == 10:
                minutes = [f"{interval*n:02}" for n in range(6) if interval*n < 60]
                for minute in minutes:
                    schedule.every(1).hour.at(f"{minute}:10").do(self.transfer_files, local_path, remote_path, remove_on_success)
            elif (interval % 60) == 0:
                hrs = [f"{n:02}:00:10" for n in range(0, 24, interval // 60)]
                for hr in hrs:
                    schedule.every(1).day.at(hr).do(self.transfer_files, local_path, remote_path, remove_on_success)
            elif interval == 1440:
                schedule.every(1).day.at("00:00:10").do(self.transfer_files, local_path, remote_path, remove_on_success)
            else:
                raise ValueError("'interval' must be 10 minutes or a multiple of 60 minutes and a maximum of 1440 minutes.")

        except Exception as err:
            self.schedule_logger.error(err)


class FTPClient:
    """
    FTP based file handling using username/password.

    Expected config keys (as in your bucdaq.yml):
        config['ftp']['host']
        config['ftp']['usr']
        config['ftp']['pwd']          # path to password file (typically relative to config['root'])
        config['ftp']['remote_path']  # remote destination root (e.g. './buc')

    Local source is derived from:
        config['root'] + config['staging']
    """

    def __init__(self, config: dict):
        try:
            # configure logging
            _logger = f"{os.path.basename(config['logging']['file'])}".split(".")[0]
            self.logger = logging.getLogger(f"{_logger}.{__name__}.ftp")
            self.schedule_logger = logging.getLogger(f"{_logger}.schedule")
            self.schedule_logger.setLevel(level=logging.DEBUG)
            self.logger.info("Initialize FTPClient")

            ftp_cfg = config["ftp"]
            self.host = ftp_cfg["host"]
            self.usr = ftp_cfg["usr"]

            # Optional knobs (sensible defaults)
            self.port = int(ftp_cfg.get("port", 21))
            self.timeout = int(ftp_cfg.get("timeout", 30))
            self.passive = bool(ftp_cfg.get("passive", True))

            # Password file: as provided in config['ftp']['pwd']
            pwd_path_cfg = ftp_cfg.get("pwd")
            if not pwd_path_cfg:
                raise ValueError("Missing config['ftp']['pwd'] (password file path).")

            # In your config this is relative to root (e.g. '.secrets/ftp_buc_password')
            pwd_path = _resolve_under_root(config["root"], pwd_path_cfg)
            self.password = self._read_password(pwd_path)

            # local source root (mirrors SFTPClient behavior)
            self.local_path = os.path.join(os.path.expanduser(config["root"]), config["staging"])

            # remote destination root
            self.remote_path = ftp_cfg["remote_path"]

        except Exception as err:
            try:
                self.logger.error(err)
            except Exception:
                pass

    def _read_password(self, password_file: str) -> str:
        p = Path(os.path.expanduser(password_file))
        if not p.exists():
            raise FileNotFoundError(f"FTP password file not found: {p}")
        pwd = p.read_text(encoding="utf-8").strip()
        if not pwd:
            raise ValueError(f"FTP password file is empty: {p}")
        return pwd

    def _open(self) -> ftplib.FTP:
        ftp = ftplib.FTP()
        ftp.connect(self.host, self.port, timeout=self.timeout)
        ftp.login(self.usr, self.password)
        ftp.set_pasv(self.passive)
        return ftp

    def is_alive(self) -> bool:
        """Test FTP login + basic command."""
        ftp = None
        try:
            ftp = self._open()
            ftp.pwd()
            return True
        except Exception as err:
            self.logger.error(err)
            return False
        finally:
            try:
                if ftp is not None:
                    ftp.quit()
            except Exception:
                pass

    def _setup_remote_path(self, ftp: ftplib.FTP, base_cwd: str, remote_dir: str) -> str:
        """
        Ensure remote_dir exists and change into it.

        Important:
            For relative remote paths (e.g. './buc'), we interpret them relative to base_cwd.
            We always reset to base_cwd before traversing, to avoid path drift between files.
        """
        remote_dir = _normalize_path(remote_dir).strip()
        remote_dir = re.sub(r"/{2,}", "/", remote_dir)

        # Reset to login directory for stable traversal
        try:
            ftp.cwd(base_cwd)
        except Exception:
            pass

        if remote_dir in ("", ".", "./"):
            return ftp.pwd()

        # Handle './something'
        if remote_dir.startswith("./"):
            remote_dir = remote_dir[2:]

        # Absolute path: try to start from '/'
        parts = [p for p in remote_dir.split("/") if p and p != "."]
        if remote_dir.startswith("/"):
            try:
                ftp.cwd("/")
            except Exception:
                # if not allowed, fall back to base_cwd already set above
                pass

        for part in parts:
            try:
                ftp.cwd(part)
            except ftplib.error_perm:
                try:
                    ftp.mkd(part)
                except ftplib.error_perm:
                    # might already exist or server forbids mkdir; try cwd anyway
                    pass
                ftp.cwd(part)

        return ftp.pwd()

    def transfer_files(self, local_path: str = str(), remote_path: str = str(), remove_on_success: bool = True) -> None:
        """
        Transfer (move) all files from local_path and sub-folders to remote_path using FTP.

        Mirrors SFTPClient.transfer_files() behaviour and folder mirroring logic:
            local root:  <root>/<staging>/...
            remote root: <remote_path>/...

        Args:
            local_path (str, optional): local root directory to walk (defaults to config root+staging)
            remote_path (str, optional): remote root directory (defaults to config['ftp']['remote_path'])
            remove_on_success (bool, optional): remove local file after upload (best-effort verification)
        """
        ftp = None
        try:
            self.transfered = []

            if not local_path:
                local_path = self.local_path
            if not remote_path:
                remote_path = self.remote_path

            local_path = _normalize_path(local_path).rstrip("/")
            remote_path = _normalize_path(remote_path).rstrip("/")

            self.logger.info(f"{local_path} > {remote_path}", extra={"to_logfile": True})

            ftp = self._open()
            base_cwd = ftp.pwd()

            top = local_path
            for root, _, files in os.walk(top=top):
                for file in files:
                    local_file = _normalize_path(os.path.join(root, file)).rstrip("/")
                    self.logger.info(f"{local_file}", extra={"to_logfile": True})

                    # Mirror relative folder structure under remote_path
                    parts = _normalize_path(root).replace(local_path, "").strip("/")
                    remote_dir = f"{remote_path}/{parts}".rstrip("/")
                    remote_file = f"{remote_dir}/{file}".rstrip("/")

                    self.logger.info(f"{remote_file}", extra={"to_logfile": True})

                    _ = self._setup_remote_path(ftp, base_cwd, remote_dir)

                    with open(local_file, "rb") as fh:
                        ftp.storbinary(f"STOR {file}", fh)

                    self.transfered.append(file)
                    self.logger.debug(f"put {local_file} > {remote_file}")

                    if remove_on_success:
                        local_size = os.stat(local_file).st_size
                        remote_size = None

                        # Some servers support SIZE; some don't. Treat failure as "unknown".
                        try:
                            remote_size = ftp.size(file)
                        except Exception:
                            remote_size = None

                        if (remote_size is None) or (int(remote_size) == int(local_size)):
                            os.remove(local_file)
                        else:
                            self.logger.warning(
                                f"local file size: {local_size}, remote file: {remote_size} differ. Did not remove {local_file}."
                            )

        except Exception as err:
            self.logger.error(f"transfer_files (FTP): {err}")
        finally:
            try:
                if ftp is not None:
                    ftp.quit()
            except Exception:
                pass

    def setup_transfer_schedules(self, local_path: str, remote_path: str, remove_on_success: bool = True, interval: int = 60):
        """
        Setup scheduled FTP transfer jobs.

        interval:
            - 10 minutes, or
            - multiple of 60 minutes, up to 1440 (daily)
        """
        try:
            local_path = _normalize_path(local_path)
            remote_path = _normalize_path(remote_path)

            if interval == 10:
                minutes = [f"{interval*n:02}" for n in range(6) if interval*n < 60]
                for minute in minutes:
                    schedule.every(1).hour.at(f"{minute}:10").do(self.transfer_files, local_path, remote_path, remove_on_success)
            elif (interval % 60) == 0:
                hrs = [f"{n:02}:00:10" for n in range(0, 24, interval // 60)]
                for hr in hrs:
                    schedule.every(1).day.at(hr).do(self.transfer_files, local_path, remote_path, remove_on_success)
            elif interval == 1440:
                schedule.every(1).day.at("00:00:10").do(self.transfer_files, local_path, remote_path, remove_on_success)
            else:
                raise ValueError("'interval' must be 10 minutes or a multiple of 60 minutes and a maximum of 1440 minutes.")

        except Exception as err:
            self.schedule_logger.error(err)


def ftp_transfer_files(config: dict, local_path: str = str(), remote_path: str = str(), remove_on_success: bool = True) -> None:
    """
    Convenience one-liner helper mirroring the style of your existing module usage.
    """
    FTPClient(config).transfer_files(local_path=local_path, remote_path=remote_path, remove_on_success=remove_on_success)


if __name__ == "__main__":
    pass
