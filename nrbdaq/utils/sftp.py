#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Manage files. Currently, sftp transfer to MeteoSwiss is supported.

@author: joerg.klausen@meteoswiss.ch
"""
import configparser
import logging
import os
import re
import shutil
import time
import zipfile
from xmlrpc.client import Boolean

import colorama
import paramiko
import sockslib


class SFTPClient:
    """
    SFTP based file handling, optionally using SOCKS5 proxy.

    Available methods include
    - is_alive():
    - localfiles():
    - stage_current_log_file():
    - stage_current_config_file():
    - setup_remote_folders():
    - put_r(): recursively put files
    - xfer_r(): recursively move files
    """

    _zip = None
    _logs = None
    _staging = None
    _logfile = None
    _log = False
    _logger = None
    _sftpkey = None
    _sftpusr = None
    _sftphost = None

    def __init__(self, config_file: str = 'nrbdaq.cfg'):
        """
        Initialize the SFTPClient class with parameters from a configuration file.

        :param config_file: Path to the configuration file.
                    config['sftp']['host']:
                    config['sftp']['usr']:
                    config['sftp']['key']:
                    config['staging']['path']: relative path of staging area
        """
        self.config = self.load_config(config_file)
        self.setup_logger()

        colorama.init(autoreset=True)
        print("# Initialize SFTPClient")

        try:
            # sftp settings
            self._sftphost = self.config['sftp']['host']
            self._sftpusr = self.config['sftp']['usr']
            self._sftpkey = paramiko.RSAKey.from_private_key_file(\
                os.path.expanduser(self.config['sftp']['key']))

            # # configure client proxy if needed
            # if self.config['sftp']['proxy']['socks5']:
            #     with sockslib.SocksSocket() as sock:
            #         sock.set_proxy((self.config['sftp']['proxy']['socks5'],
            #                         self.config['sftp']['proxy']['port']), sockslib.Socks.SOCKS5)

            # configure staging
            self._staging = os.path.expanduser(self.config['staging']['path'])
            self._staging = re.sub(r'(/?\.?\\){1,2}', '/', self._staging)
            self._zip = self.config['staging']['zip']

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)

    @staticmethod
    def load_config(config_file: str) -> configparser.ConfigParser:
        """
        Load configuration from a file.

        :param config_file: Path to the configuration file.
        :return: ConfigParser object with the loaded configuration.
        """
        config = configparser.ConfigParser()
        config.read(config_file)
        return config

    def setup_logger(self):
        """
        Setup logger for the SFTPClient class.
        """
        log_file = self.config['logging']['log_file']
        log_level = self.config['logging']['log_level'].upper()

        logging.basicConfig(
            filename=log_file,
            level=getattr(logging, log_level, logging.ERROR),
            format='%(asctime)s:%(levelname)s:%(message)s'
        )
        logging.getLogger('paramiko.transport').setLevel(level=logging.ERROR)
        paramiko.util.log_to_file(os.path.join(self._logs, "paramiko.log"))


    def is_alive(self) -> bool:
        """Test ssh connection to sftp server.

        Returns:
            bool: [description]
        """
        try:
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self._sftphost, username=self._sftpusr, pkey=self._sftpkey)

                with ssh.open_sftp() as sftp:
                    sftp.close()
            return True
        except Exception as err:
            print(err)
            return False


    def localfiles(self, localpath=None) -> list:
        """Establish list of local files.

        Args:
            localpath ([type], optional): [description]. Defaults to None.

        Returns:
            list: [description]
        """
        fnames = []
        dnames = []
        onames = []

        if localpath is None:
            localpath = self._staging

        # def store_files_name(name):
        #     fnames.append(name)

        # def store_dir_name(name):
        #     dnames.append(name)

        # def store_other_file_types(name):
        #     onames.append(name)

        try:
            root, dnames, fnames = os.walk(localpath)
            fnames = [re.sub(r'(/?\.?\\){1,2}', '/', s) for s in fnames]

            return fnames

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def stage_current_log_file(self) -> None:
        """
        Stage the most recent file.

        :return:
        """
        try:
            root = os.path.join(self._staging, os.path.basename(self._logs))
            os.makedirs(root, exist_ok=True)
            if self._zip:
                # create zip file
                archive = os.path.join(root, "".join([os.path.basename(self._logfile[:-4]), ".zip"]))
                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
                    fh.write(self._logfile, os.path.basename(self._logfile))
            else:
                shutil.copyfile(self._logfile, os.path.join(root, os.path.basename(self._logfile)))

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def stage_current_config_file(self, config_file: str) -> None:
        """
        Stage the most recent file.

        :param: str config_file: path to config file
        :return:
        """
        try:
            os.makedirs(self._staging, exist_ok=True)
            if self._zip:
                # create zip file
                archive = os.path.join(self._staging, "".join([os.path.basename(\
                    config_file[:-4]), ".zip"]))
                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
                    fh.write(config_file, os.path.basename(config_file))
            else:
                shutil.copyfile(config_file, os.path.join(\
                    self._staging, os.path.basename(config_file)))

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def put(self, localpath, remotepath) -> None:
        """Send a file to a remotehost using SFTP and SSH.

        Args:
            localpath (str): full path to local file
            remotepath (str): relative path to remotefile
        """
        try:
            remotepath = re.sub(r'(/?\.?\\){1,2}', '/', remotepath)
            msg = f"{time.strftime('%Y-%m-%d %H:%M:%S')} .put {localpath} > {remotepath}"
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self._sftphost, username=self._sftpusr, pkey=self._sftpkey)
                with ssh.open_sftp() as sftp:
                    sftp.put(localpath=localpath, remotepath=remotepath, confirm=True)
                    sftp.close()
                print(msg)
                self._logger.info(msg)

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def remote_item_exists(self, remoteitem) -> Boolean:
        """Check on remote server if an item exists. Assume this indicates successful transfer.

        Args:
            remoteitem (str): path to remote item

        Returns:
            Boolean: True if item exists, False otherwise.
        """
        try:
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self._sftphost, username=self._sftpusr, pkey=self._sftpkey)
                with ssh.open_sftp() as sftp:
                    if sftp.stat(remoteitem).size > 0:
                        return True
                    else:
                        return False
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def setup_remote_folders(self, localpath=None, remotepath=None) -> None:
        """
        Determine directory structure under localpath and replicate on remote host.

        :param str localpath:
        :param str remotepath:
        :return: Nothing
        """
        try:
            if localpath is None:
                localpath = self._staging

            # sanitize localpath
            localpath = re.sub(r'(/?\.?\\){1,2}', '/', localpath)

            if remotepath is None:
                remotepath = '.'

            # sanitize remotepath
            remotepath = re.sub(r'(/?\.?\\){1,2}', '/', remotepath)

            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} .setup_remote_folders (source: {localpath}, target: {remotepath})")

            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self._sftphost, username=self._sftpusr, pkey=self._sftpkey)
                with ssh.open_sftp() as sftp:
                    # determine local directory structure, establish same structure on remote host
                    for dirpath, dirnames, filenames in os.walk(top=localpath):
                        dirpath = re.sub(r'(/?\.?\\){1,2}', '/', dirpath).replace(localpath, remotepath)
                        try:
                            sftp.mkdir(dirpath, mode=16877)
                        except OSError:
                            pass
                    sftp.close()

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def xfer_r(self, localpath=None, remotepath=None) -> None:
        """
        Recursively transfer (move) all files from localpath to remotepath. Note: At present, parent elements of remote path must already exist.

        :param str localpath:
        :param str remotepath:
        :param bln preserve_mtime: see pysftp documentation
        :return: Nothing
        """
        try:
            if localpath is None:
                localpath = self._staging

            # sanitize localpath
            # localpath = re.sub(r'(/?\.?\\){1,2}', '/', localpath)

            if remotepath is None:
                remotepath = '.'

            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} .xfer_r (source: {localpath}, target: {self._sftphost}/{self._sftpusr}/{remotepath})")

            localitem = None
            remoteitem = None
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self._sftphost, username=self._sftpusr, pkey=self._sftpkey)
                with ssh.open_sftp() as sftp:
                    # walk local directory structure, put file to remote location
                    for dirpath, dirnames, filenames in os.walk(top=localpath):
                        for filename in filenames:
                            localitem = os.path.join(dirpath, filename)
                            remoteitem = os.path.join(dirpath.replace(localpath, remotepath), filename)
                            remoteitem = re.sub(r'(\\){1,2}', '/', remoteitem)
                            msg = "%s .put %s > %s" % (time.strftime('%Y-%m-%d %H:%M:%S'),
                                                       localitem.replace(localpath, ''), remoteitem)
                            res = sftp.put(localpath=localitem, remotepath=remoteitem, confirm=True)
                            print(msg)
                            self._logger.info(msg)

                            # remove local file if it exists on remote host.
                            try:
                                localsize = os.stat(localitem).st_size
                                remotesize = res.st_size
                                print("localitem size: %s, remoteitem size: %s" % (localsize, remotesize))
                                if remotesize == localsize:
                                    os.remove(localitem)
                            except Exception as err:
                                msg = "%s %s not found on remote host, will try again later." % (time.strftime('%Y-%m-%d %H:%M:%S'), remoteitem)
                                print(colorama.Fore.RED + msg)
                                if self._log:
                                    self._logger.info(msg)
                                    self._logger.error(err)

        except Exception as err:
            msg = "%s %s > %s failed." % (time.strftime('%Y-%m-%d %H:%M:%S'), localitem, remoteitem)
            print(colorama.Fore.RED + msg)
            if self._log:
                self._logger.info(msg)
                self._logger.error(err)


if __name__ == "__main__":
    pass
