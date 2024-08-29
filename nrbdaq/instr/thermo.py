# -*- coding: utf-8 -*-
"""
Define a class TEI49I facilitating communication with a Thermo TEI49i instrument.

@author: joerg.klausen@meteoswiss.ch
"""
import os
from datetime import datetime, timedelta
import logging
import shutil
import socket
import re
# import serial
import schedule
import time
import zipfile

import colorama

# from mkndaq.utils import datetimebin


class Thermo49i:
    def __init__(self, config: dict, name: str='49i'):
        """
        Initialize the Thermo 49i instrument class with parameters from a configuration file.

        Args:
            config (dict): general configuration
        """
        colorama.init(autoreset=True)

        try:
            # configure logging
            _logger = f"{os.path.basename(config['logging']['file'])}".split('.')[0]
            self.logger = logging.getLogger(f"{_logger}.{__name__}")

            # read instrument control properties for later use
            self._name = name
            self._id = config[name]['id'] + 128
            self._serial_number = config[name]['serial_number']
            self._get_config = config[name]['get_config']
            self._set_config = config[name]['set_config']
            self._get_data = config[name]['get_data']
            # self._data_header = config[name]['data_header']
            self._data_header = 'pcdate pctime time date flags o3 hio3 cellai cellbi bncht lmpt o3lt flowa flowb pres'

            self.logger.info(f"# Initialize Thermo 49i (name: {self._name}  S/N: {self._serial_number})")

            self._serial_com = config.get(name, {}).get('serial', None)
            if self._serial_com:
                # configure serial port
                port = config[name]['port']
                # self._serial = serial.Serial(port=port,
                #                             baudrate=config[port]['baudrate'],
                #                             bytesize=config[port]['bytesize'],
                #                             parity=config[port]['parity'],
                #                             stopbits=config[port]['stopbits'],
                #                             timeout=config[port]['timeout'])
                # if self._serial.is_open:
                #     self._serial.close()
                # self.logger.info(f"Serial port {port} successfully opened and closed.")
            else:
                # configure tcp/ip
                self._sockaddr = (config[name]['socket']['host'],
                                config[name]['socket']['port'])
                self._socktout = config[name]['socket']['timeout']
                self._socksleep = config[name]['socket']['sleep']

            root = os.path.expanduser(config['root'])

            # configure data collection
            self._sampling_interval = config[name]['sampling_interval']
            schedule.every(int(self._sampling_interval)).minutes.at(':00').do(self.get_data)
                     
            # configure saving, staging and transfer
            self.data_path = os.path.join(root, config[name]['data'])
            os.makedirs(self.data_path, exist_ok=True)
            self.staging_path = os.path.join(root, config[name]['staging'])
            os.makedirs(self.staging_path, exist_ok=True)

            self.reporting_interval = config[name]['reporting_interval']
            if self.reporting_interval==10:
                schedule.every(10).minutes.at(':01').do(self._save_and_stage_data)
            elif self.reporting_interval==60:
                schedule.every().hour.at('00:02').do(self._save_and_stage_data)
            elif self.reporting_interval==1440:
                schedule.every().day.at('00:00:03').do(self._save_and_stage_data)
            else:
                raise ValueError('reporting_interval must be either 10 or 60 or 1440 minutes.')
            # _reporting_interval = int(self.reporting_interval / 60)
            # if (self.reporting_interval % 60)==0 and _reporting_interval in range(24):
            #     hours = [f"{self.reporting_interval*n:02}:00:03" for n in range(24) if self.reporting_interval*n < 24]
            #     for hr in hours:
            #         schedule.every().day.at(hr).do(self._save_and_stage_data)                
            # else:
            #     raise ValueError('reporting_interval must be either 10 or (1 .. 24)x60 minutes.')

            # configure archive
            self.archive_path = os.path.join(root, config[name]['archive'])
            os.makedirs(self.archive_path, exist_ok=True)

            # configure remote transfer
            self.remote_path = config[name]['remote_path']

            # initialize data response and datetime stamp           
            self._data = str()
            self._dtm = None

            # self.get_config()
            # self.set_config()

        except Exception as err:
            self.logger.error(err)
            pass


    def tcpip_comm(self, cmd: str, tidy=True) -> str:
        """
        Send a command and retrieve the response. Assumes an open connection.

        :param cmd: command sent to instrument
        :param tidy: remove cmd echo, \n and *\r\x00 from result string, terminate with \n
        :return: response of instrument, decoded
        """
        _id = bytes([self._id])
        rcvd = b''
        try:
            # open socket connection as a client
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM, ) as s:
                # connect to the server
                s.settimeout(self._socktout)
                s.connect(self._sockaddr)

                if self._simulate:
                    _id = b''

                # send data
                s.sendall(_id + (f"{cmd}\x0D").encode())
                time.sleep(self._socksleep)

                # receive response
                while True:
                    data = s.recv(1024)
                    rcvd = rcvd + data
                    if b'\x0D' in data:
                        break

            # decode response, tidy
            rcvd = rcvd.decode()
            if tidy:
                # - remove checksum after and including the '*'
                rcvd = rcvd.split("*")[0]
                # - remove echo before and including '\n'
                rcvd = rcvd.replace(f"{cmd}\n", "")

            return rcvd

        except Exception as err:
            self.logger.error(err)


    def serial_comm(self, cmd: str, tidy=True) -> str:
        """
        Send a command and retrieve the response. Assumes an open connection.

        :param cmd: command sent to instrument
        :param tidy: remove echo and checksum after '*'
        :return: response of instrument, decoded
        """
        __id = bytes([self._id])
        rcvd = b''
        try:
            self._serial.write(__id + (f"{cmd}\x0D").encode())
            time.sleep(0.5)
            while self._serial.in_waiting > 0:
                rcvd = rcvd + self._serial.read(1024)
                time.sleep(0.1)

            rcvd = rcvd.decode()
            if tidy:
                # - remove checksum after and including the '*'
                rcvd = rcvd.split("*")[0]
                # - remove echo before and including '\n'
                if cmd.join("\n") in rcvd:
                    # rcvd = rcvd.split("\n")[1]
                    rcvd = rcvd.replace(cmd, "")
                # remove trailing '\r\n'
                # rcvd = rcvd.rstrip()
                rcvd = rcvd.strip()
            return rcvd

        except Exception as err:
            self.logger.error(err)
            print(err)


    def get_config(self) -> list:
        """
        Read current configuration of instrument and optionally write to log.

        :return (err, cfg) configuration or errors, if any.

        """
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} .get_config (name={self._name})")
        cfg = []
        try:
            for cmd in self._get_config:
                if self._serial_com:
                    cfg.append(self.serial_comm(cmd))
                else:
                    cfg.append(self.tcpip_comm(cmd))

            if self._log:
                self._logger.info(f"Current configuration of '{self._name}': {cfg}")

            return cfg

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def set_datetime(self) -> None:
        """
        Synchronize date and time of instrument with computer time.

        :return:
        """
        try:
            cmd = f"set date {time.strftime('%m-%d-%y')}"
            if self._serial_com:
                dte = self.serial_comm(cmd)
            else:
                dte = self.tcpip_comm(cmd)
            msg = f"Date of instrument {self._name} set to: {dte}"
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}")
            self._logger.info(msg)

            cmd = f"set time {time.strftime('%H:%M:%S')}"
            if self._serial_com:
                tme = self.serial_comm(cmd)
            else:
                tme = self.tcpip_comm(cmd)
            msg = f"Time of instrument {self._name} set to: {tme}"
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}")
            self._logger.info(msg)

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def set_config(self) -> list:
        """
        Set configuration of instrument and optionally write to log.

        :return (err, cfg) configuration set or errors, if any.
        """
        print("%s .set_config (name=%s)" % (time.strftime('%Y-%m-%d %H:%M:%S'), self._name))
        cfg = []
        try:
            for cmd in self._set_config:
                if self._serial_com:
                    cfg.append(self.serial_comm(cmd))
                else:
                    cfg.append(self.tcpip_comm(cmd))
                time.sleep(1)

            if self._log:
                self._logger.info(f"Configuration of '{self._name}' set to: {cfg}")

            return cfg

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def get_data(self):
        """
        Send command, retrieve response from instrument and append to self._data.
        """
        try:
            # if self._serial_com:
            #     _ = self.serial_comm(self._get_data)
            # else:
            #     _ = self.tcpip_comm(self._get_data)
            _ = self.tcpip_comm(self._get_data)
            self.logger.info(f"{self._name}: {_}"),
            self._data += _

            return

        except Exception as err:
            self.logger.error(err)


    def get_all_lrec(self, save=True) -> str:
        """download entire buffer from instrument and save to file

        :param bln save: Should data be saved to file? default=True
        :return str response as decoded string
        """
        try:
            dtm = time.strftime('%Y-%m-%d %H:%M:%S')

            # retrieve numbers of lrec stored in buffer
            cmd = "no of lrec"
            if self._serial_com:
                no_of_lrec = self.serial_comm(cmd)
            else:
                no_of_lrec = self.tcpip_comm(cmd)
            no_of_lrec = int(re.findall(r"(\d+)", no_of_lrec)[0])

            if save:
                # generate the datafile name
                self.__datafile = os.path.join(self.__datadir,
                                            "".join([self._name, "_all_lrec-",
                                                    time.strftime("%Y%m%d%H%M%S"), ".dat"]))

            # retrieve all lrec records stored in buffer
            index = no_of_lrec
            retrieve = 10

            while index > 0:
                if index < 10:
                    retrieve = index
                cmd = f"lrec {str(index)} {str(retrieve)}"
                print(cmd)
                if self._serial_com:
                    data = self.serial_comm(cmd)
                else:
                    data = self.tcpip_comm(cmd)

                # remove all the extra info in the string returned
                # 05:26 07-19-22 flags 0C100400 o3 30.781 hio3 0.000 cellai 50927 cellbi 51732 bncht 29.9 lmpt 53.1 o3lt 0.0 flowa 0.435 flowb 0.000 pres 493.7
                data = data.replace("flags ", "")
                data = data.replace("hio3 ", "")
                data = data.replace("cellai ", "")
                data = data.replace("cellbi ", "")
                data = data.replace("bncht ", "")
                data = data.replace("lmpt ", "")
                data = data.replace("o3lt ", "")
                data = data.replace("flowa ", "")
                data = data.replace("flowb ", "")
                data = data.replace("pres ", "")
                data = data.replace("o3 ", "")

                if save:
                    if not os.path.exists(self.__datafile):
                        # if file doesn't exist, create and write header
                        with open(self.__datafile, "at", encoding='utf8') as fh:
                            fh.write(f"{self._data_header}\n")
                            fh.close()

                    with open(self.__datafile, "at", encoding='utf8') as fh:
                        fh.write(f"{data}\n")
                        fh.close()

                index = index - 10

            if save:
                # stage data for transfer
                root = os.path.join(self.__staging, os.path.basename(self.__datadir))
                os.makedirs(root, exist_ok=True)
                if self.__zip:
                    # create zip file
                    archive = os.path.join(root, "".join([os.path.basename(self.__datafile[:-4]), ".zip"]))
                    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
                        fh.write(self.__datafile, os.path.basename(self.__datafile))
                else:
                    shutil.copyfile(self.__datafile, os.path.join(root, os.path.basename(self.__datafile)))

            return data

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def get_o3(self) -> str:
        try:
            if self._serial_com:
                return self.serial_comm('o3')
            else:
                return self.tcpip_comm('o3')

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def print_o3(self) -> None:
        try:
            if self._serial_com:
                o3 = self.serial_comm('O3').split()
            else:
                o3 = self.tcpip_comm('O3').split()
            print(colorama.Fore.GREEN + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self._name}] {o3[0].upper()} {str(float(o3[1]))} {o3[2]}")

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(colorama.Fore.RED + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self._name}] produced error {err}.")


    def _save_and_stage_data(self):
        try:
            if self._data:
                if self.reporting_interval==10:
                    timestamp = datetime.now().strftime('%Y%m%d%H%M')
                elif self.reporting_interval==60:
                    timestamp = datetime.now().strftime('%Y%m%d%H')
                elif self.reporting_interval==1440:
                    timestamp = datetime.now().strftime('%Y%m%d')
                
                file = os.path.join(self.data_path, f"49i-{timestamp}.dat")
                if os.path.exists(file):
                    mode = 'a'
                else:
                    mode = 'w'
                    self.logger.info(f"# Writing to {self.data_path}/49i-{timestamp}.dat")
                
                # open file and write to it
                with open(file=file, mode=mode) as fh:
                    fh.write(f"{self._dtm}, {self._data}\n")
                
                # reset self._data
                self._data = str()

                # create zip file and stage it
                archive = os.path.join(self.staging_path, os.path.basename(file).replace('.dat', '.zip'))
                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.write(file, os.path.basename(file))
                self.logger.debug(f"file staged: {archive}")

        except Exception as err:
            self.logger.error(err)


if __name__ == "__main__":
    pass
