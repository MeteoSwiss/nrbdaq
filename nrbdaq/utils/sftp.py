#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Manage files. Currently, sftp transfer to MeteoSwiss is supported.

@author: joerg.klausen@meteoswiss.ch
"""
import logging
import os
import re
# import time
from xmlrpc.client import Boolean

import colorama
import paramiko
import schedule
# import sockslib


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
    staging = None
    key = None
    usr = None
    host = None

    def __init__(self, config: dict):
        """
        Initialize the SFTPClient class with parameters from a configuration file.

        :param config_file: Path to the configuration file.
                    config['sftp']['host']:
                    config['sftp']['usr']:
                    config['sftp']['key']:
                    config['staging']['root']: relative path of staging area
        """
        colorama.init(autoreset=True)

        try:
            # configure logging
            _logger = f"{os.path.basename(config['logging']['file'])}".split('.')[0]
            self.logger = logging.getLogger(f"{_logger}.{__name__}")
            self.logger.info("Initialize SFTPClient")

            # sftp settings
            self.host = config['sftp']['host']
            self.usr = config['sftp']['usr']
            self.key = paramiko.RSAKey.from_private_key_file(\
                os.path.expanduser(config['sftp']['key']))

            # # configure client proxy if needed
            # if self.config['sftp']['proxy']['socks5']:
            #     with sockslib.SocksSocket() as sock:
            #         sock.set_proxy((self.config['sftp']['proxy']['socks5'],
            #                         self.config['sftp']['proxy']['port']), sockslib.Socks.SOCKS5)

            # configure staging
            self.staging = os.path.expanduser(config['staging']['root'])
            self.staging = re.sub(r'(/?\.?\\){1,2}', '/', self.staging)

            # configure transfer schedule
            self.reporting_interval = config['data']['reporting_interval']
            if self.reporting_interval=='daily':
                schedule.every(1).days.at('00:01:00').do(self.transfer_all_files)
            elif self.reporting_interval=='hourly':
                schedule.every(1).hours.at('00:01').do(self.transfer_all_files)
            else:
                raise ValueError('reporting_interval must be one of daily|hourly.')

            # self._zip = self.config['staging']['zip']

        except Exception as err:
            self.logger.error(err)


    def is_alive(self) -> bool:
        """Test ssh connection to sftp server.

        Returns:
            bool: [description]
        """
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


    def localfiles(self, localpath: str=None) -> list[str]:
        """Establish list of local files.

        Args:
            localpath ([type], optional): [description]. Defaults to None.

        Returns:
            list: [description]
        """
        fnames = list()

        if localpath is None:
            localpath = self.staging

        try:
            root, dnames, fnames = os.walk(localpath)
            fnames = [os.path.join(root, re.sub(r'(/?\.?\\){1,2}', '/', s)) for s in fnames]
            return fnames

        except Exception as err:
            self.logger.error(err)


    def remote_item_exists(self, remoteitem: str) -> Boolean:
        """Check on remote server if an item exists. Assume this indicates successful transfer.

        Args:
            remoteitem (str): path to remote item

        Returns:
            Boolean: True if item exists, False otherwise.
        """
        try:
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)
                with ssh.open_sftp() as sftp:
                    if sftp.stat(remoteitem).size > 0:
                        return True
                    else:
                        return False
        except Exception as err:
            self.logger.error(err)


    def list_remote_items(self, remotepath: str='/gaw_mkn'):
        try:
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)
                with ssh.open_sftp() as sftp:
                    return sftp.listdir(remotepath)

        except Exception as err:
            self.logger.error(err)


    def setup_remote_folders(self, localpath: str=str(), remotepath: str=str()) -> None:
        """
        Determine directory structure under localpath and replicate on remote host.

        :param str localpath:
        :param str remotepath:
        :return: Nothing
        """
        try:
            if localpath is None:
                localpath = self.staging

            # sanitize localpath
            localpath = re.sub(r'(/?\.?\\){1,2}', '/', localpath)

            if remotepath is None:
                remotepath = '.'

            # sanitize remotepath
            remotepath = re.sub(r'(/?\.?\\){1,2}', '/', remotepath)

            self.logger.info(f".setup_remote_folders (source: {localpath}, target: {remotepath})")

            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)
                with ssh.open_sftp() as sftp:
                    # determine local directory structure, establish same structure on remote host
                    for dirpath, dirnames, filenames in os.walk(top=localpath):
                        dirpath = re.sub(r'(/?\.?\\){1,2}', '/', dirpath).replace(localpath, remotepath)
                        try:
                            sftp.mkdir(dirpath, mode=16877)
                        except OSError as err:
                            self.logger.error(err)
                            pass
                    sftp.close()

        except Exception as err:
            self.logger.error(err)


    def put_file(self, localpath: str, remotepath: str) -> None:
        """Send a file to a remotehost using SFTP and SSH.

        Args:
            localpath (str): full path to local file
            remotepath (str): relative path to remotefile
        """
        try:
            if os.path.exists(localpath):
                remotepath = re.sub(r'(/?\.?\\){1,2}', '/', remotepath)
                msg = f".put {localpath} > {remotepath}"
                with paramiko.SSHClient() as ssh:
                    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)
                    with ssh.open_sftp() as sftp:
                        res = sftp.put(localpath=os.path.abspath(os.path.expanduser(localpath)), remotepath=remotepath, confirm=True)
                        sftp.close()
                    self.logger.info(msg)
            else:
                raise ValueError("localpath does not exist.")
        except Exception as err:
            self.logger.error(err)


    def transfer_all_files(self, localpath=None, remotepath=None) -> None:
        """
        Recursively transfer (move) all files from localpath to remotepath. Note: At present, parent elements of remote path must already exist.

        :param str localpath:
        :param str remotepath:
        :param bln preserve_mtime: see pysftp documentation
        :return: Nothing
        """
        try:
            if localpath is None:
                localpath = self.staging

            # sanitize localpath
            # localpath = re.sub(r'(/?\.?\\){1,2}', '/', localpath)

            if remotepath is None:
                remotepath = '.'

            self.logger.info(f".transfer_all_files (source: {localpath}, target: {self.host}/{self.usr}/{remotepath})")

            localitem = None
            remoteitem = None
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)
                with ssh.open_sftp() as sftp:
                    # walk local directory structure, put file to remote location
                    for dirpath, dirnames, filenames in os.walk(top=localpath):
                        for filename in filenames:
                            localitem = os.path.join(dirpath, filename)
                            remoteitem = os.path.join(dirpath.replace(localpath, remotepath), filename)
                            remoteitem = re.sub(r'(\\){1,2}', '/', remoteitem)
                            msg = f".put {localitem.replace(localpath, '')} > {remoteitem}"
                            res = sftp.put(localpath=localitem, remotepath=remoteitem, confirm=True)
                            self.logger.info(msg)

                            # remove local file from staging if it exists on remote host.
                            try:
                                localsize = os.stat(localitem).st_size
                                remotesize = res.st_size
                                self.logger.debug("localitem size: %s, remoteitem size: %s" % (localsize, remotesize))
                                if remotesize == localsize:
                                    os.remove(localitem)
                            except Exception as err:
                                msg = f"{remoteitem} not found on remote host, will try again later."
                                self.logger.info(colorama.Fore.RED + msg)
                                self.logger.warning(msg)

        except Exception as err:
            msg = f"{localitem} > {remoteitem} failed with {err}."
            self.logger.info(colorama.Fore.RED + msg)
            self.logger.error(err)


if __name__ == "__main__":
    pass
