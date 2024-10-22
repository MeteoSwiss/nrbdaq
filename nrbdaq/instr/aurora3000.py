import logging
import os
import time
import zipfile
from datetime import datetime, timedelta
from typing import List, Tuple

import numpy as np
import polars as pl
import schedule
import serial

from nrbdaq.utils.utils import load_config, setup_logging


class Aurora3000:
    def __init__(self, config: dict):
        """
        Initialize the Aurora 3000 instrument class with parameters from a configuration file.

        :param config_file: Path to the configuration file.
        """
        try:
            # configure logging
            _logger = f"{os.path.basename(config['logging']['file'])}".split('.')[0]
            self.logger = logging.getLogger(f"{_logger}.{__name__}")
            self.logger.info("Initialize Aurora 3000 nephelometer")
            
            # configure serial port
            self.port = config['Aurora3000']['serial_port']
            self.baudrate = int(config['Aurora3000']['serial_baudrate'])
            self.timeout = float(config['Aurora3000']['serial_timeout'])
            
            # self.sampling_interval = int(config['Aurora3000']['sampling_interval'])
            # self.serial_conn = serial.Serial(self.port, self.baudrate, timeout=self.timeout)
            
            root = os.path.expanduser(config['root'])

            # configure data collection and saving
            self.sampling_interval = int(config['Aurora3000']['sampling_interval'])
            self.reporting_interval = int(config['Aurora3000']['reporting_interval'])
            # if not (self.reporting_interval % 60)==0 and self.reporting_interval<=1440:
            #     raise ValueError('reporting_interval must be a multiple of 60 and less or equal to 1440 minutes.')

            self.data_path = os.path.join(root, config['Aurora3000']['data'])
            os.makedirs(self.data_path, exist_ok=True)
                     
            # configure staging
            self.staging_path = os.path.join(root, config['Aurora3000']['staging'])

            # configure remote transfer
            self.remote_path = config['Aurora3000']['remote_path']
           
            self.current_hour = datetime.now().hour

            # store readings and timestamp
            # initialize data response and datetime stamp           
            self._instant_readings = []
            self._dio_states = []
            self._last_timestamp = None
            self._data = str()
            self._dtm = None
            self.data_file = str()

        except serial.SerialException as err:
            self.logger.error(f"Serial communication error: {err}")
            pass
        except Exception as err:
            self.logger.error(f"General error: {err}")
            pass


    def setup_schedules(self):
        try:
            # configure folders needed
            os.makedirs(self.data_path, exist_ok=True)
            os.makedirs(self.staging_path, exist_ok=True)
            # os.makedirs(self.archive_path, exist_ok=True)

            # configure data acquisition
            # collect readings every 5 seconds
            schedule.every(5).seconds.do(self.accumulate_instant_readings)
            # compute average every sampling_interval minute(s)
            schedule.every(self.sampling_interval).minutes.at(':00').do(self.accumulate_averages)            
            
            # configure saving and staging schedules
            if self.reporting_interval==10:
                self._file_timestamp_format = '%Y%m%d%H%M'
                minutes = [f"{self.reporting_interval*n:02}" for n in range(6) if self.reporting_interval*n < 6]
                for minute in minutes:
                    schedule.every(1).hour.at(f"{minute}:01").do(self._save_and_stage_data)
            elif self.reporting_interval==60:
                self._file_timestamp_format = '%Y%m%d%H'
                schedule.every(1).hour.at('00:02').do(self._save_and_stage_data)
            elif self.reporting_interval==1440:
                self._file_timestamp_format = '%Y%m%d'
                schedule.every(1).day.at('00:00:02').do(self._save_and_stage_data)

            # configure archive
            # self.archive_path = os.path.join(root, config['Aurora3000']['archive'])
            # os.makedirs(self.archive_path, exist_ok=True)


        except Exception as err:
            self.logger.error(err)


    def serial_comm(self, cmd: str, sep: str=',') -> str:
        try:
            data = bytes()
            with serial.Serial(self.port, self.baudrate, 8, 'N', 1, self.timeout) as ser:
                ser.write(f"{cmd}\r".encode())
                time.sleep(0.2)
                while ser.in_waiting > 0:
                    data += ser.read(1024)
                    time.sleep(0.1)
                data = data.decode("utf-8")
                data = data.replace('\r\n\n', '\r\n').replace(", ", ",").replace(",", sep)
            return data
        except Exception as err:
            self.logger.error(err)


    def read_new_data(self, sep: str=',') -> str:
        try:
           return self.serial_comm('***D')
        except Exception as err:
            self.logger.error(err)


    def get_instrument_id(self, sep: str=',') -> str:
        try:
           return self.serial_comm('ID0')
        except Exception as err:
            self.logger.error(err)


    def get_current_data(self, sep: str=',') -> str:
        try:
           return self.serial_comm('VI099')
        except Exception as err:
            self.logger.error(err)


    def get_status_word(self, sep: str=',') -> str:
        try:
           return self.serial_comm('VI088')
        except Exception as err:
            self.logger.error(err)


    def parse_current_data(self, reading: str) -> Tuple[datetime, np.ndarray]:
        """Parses a comma-separated reading string into a datetime object and a numpy array of values."""
        try:
            parts = reading.split(',')
            timestamp = datetime.strptime(parts[0], "%Y-%m-%d %H:%M:%S")
            values = list(map(float, parts[1:-1]))
            values.append(int(parts[-1], 16))    # Convert last element from hex to decimal
            return timestamp, values
        except Exception as err:
            self.logger.error(err)


    def _round_to_full_minute(self, timestamp: datetime) -> datetime:
        """Rounds a datetime object to the nearest full minute."""
        try:
            if timestamp.second >= 30:
                timestamp += timedelta(minutes=1)
            return timestamp.replace(second=0, microsecond=0)
        except Exception as err:
            self.logger.error(err)


    def accumulate_instant_readings(self) -> None:
        """Collects a single reading and appends it to the self._instant_readings list."""
        try:
            reading_str = self.get_current_data()  # Assuming get_readings returns a string
            timestamp, values = self.parse_current_data(reading_str)
            self._last_timestamp = timestamp
            self._instant_readings.append(values)
            self.logger.debug(reading_str)
        except Exception as err:
            self.logger.error(err)


    def accumulate_averages(self) -> None:
        """Computes the average of the collected self._instant_readings and returns the result in the specified format."""
        try:
            if self._instant_readings:
                # Stack self._instant_readings and compute mean across columns
                readings_array = np.stack(self._instant_readings)
                averages = np.mean(readings_array, axis=0)
                
                # Round the last timestamp to the nearest full minute
                dtm = self._round_to_full_minute(self._last_timestamp)
                
                # Clear the self._instant_readings for the next 1-minute collection
                self._instant_readings = []
                
                # Return the rounded timestamp followed by the averaged values
                current_averages = ",".join(f"{avg:.3f}" for avg in averages)
                self._data = f"{self._data}{dtm.strftime('%Y-%m-%d %H:%M:%S')},{current_averages}\n"
                self.logger.info(f"Aurora3000, {current_averages[:60]}[...]")
            return
                # return f"{dtm.strftime('%Y-%m-%d %H:%M:%S')}," + ",".join(f"{avg:.2f}" for avg in averages)
        except Exception as err:
            self.logger.error(err)


    def _save_data(self) -> None:
        try:
            data_file = str()
            self.data_file = str()
            if self._data:
                # create appropriate file name and write mode
                timestamp = datetime.now().strftime(self._file_timestamp_format)               
                data_file = os.path.join(self.data_path, f"aurora3000-{timestamp}.csv")

                # configure file mode, open file and write to it
                if os.path.exists(self.data_file):
                    mode = 'a'
                    header = str()
                else:
                    mode = 'w'
                    header = 'date,time,ssp1,ssp2,ssp3,sbsp1,sbsp2,sbsp3,sample_temp,enclosure_temp,RH,pressure,major_state,DIO_state\n'
                
                with open(file=data_file, mode=mode) as fh:
                    fh.write(header)
                    fh.write(self._data)
                    self.logger.info(f"file saved: {data_file}")
            
                # reset self._data
                self._data = str()

            self.data_file = data_file
            return

        except Exception as err:
            self.logger.error(err)


    def _stage_file(self):
        """ Create zip file from self.data_file and stage archive.
        """
        try:
            if self.data_file:
                archive = os.path.join(self.staging_path, os.path.basename(self.data_file).replace('.dat', '.zip'))
                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.write(self.data_file, os.path.basename(self.data_file))
                    self.logger.info(f"file staged: {archive}")

        except Exception as err:
            self.logger.error(err)


    def _save_and_stage_data(self):
        self._save_data()
        self._stage_file()


    # def get_all_data(self, verbosity: int=0) -> str:
    #     """
    #     Rewind the pointer of the data logger to the first entry, then retrieve all data (cf. B.4 ***R, B.3 ***D). 
    #     This only works with the legacy protocol (and doesn't work very well with the NE-300).

    #     Parameters:
    #         verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

    #     Returns:
    #         str: response
    #     """
    #     try:
    #         if self.__protocol=="acoem":
    #             warnings.warn("Not implemented. Use 'get_logged_data' with specified period instead.")
    #         elif self.__protocol=='legacy':
    #             self.tcpip_comm_wait_for_line()
    #             response = self.tcpip_comm(message=f"***R\r".encode(), verbosity=verbosity).decode()
    #             response = self.get_new_data(verbosity=verbosity)
    #             # response = self.tcpip_comm(message=f"***D\r".encode(), verbosity=verbosity).decode()
    #             return response
    #         else:
    #             raise ValueError("Protocol not implemented.")
    #         return str()
    #     except Exception as err:
    #         if self._log:
    #             self._logger.error(err)
    #         print(err)
    #         return str()


    # def get_current_data(self, add_params: list=[], strict: bool=False, sep: str=' ', verbosity: int=0) -> dict:
    #     """
    #     Retrieve latest near-real-time reading on one line.
    #     With the legacy protocol, this uses the command 99 (cf. B.7 VI: 99), returning parameters [80,81,30,2,31,3,32,17,18,16,19,00,90].
    #     These are mapped to the corresponding Acoem parameters (cf. A.4 List of Aurora parametes) [1,1635000,1525000,1450000,1635090,1525090,1450090,5001,5004,5003,5002,4036,4035].
    #     Optionally, several more parameters can be retrieved, depending on the protocol.

    #     Parameters:
    #         add_params (list, optional): read more values and append to dictionary. Defaults to [].
    #         strict (bool, optional): If True, the dictionary returned is {99: response}, where response is the <sep>-separated response of the VI<serial_id>99 legacy command. Defaults to False.
    #         sep (str, optional): Separator applied if strict=True. Defaults to ' '.
    #         verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

    #     Returns:
    #         dict: Dictionary of parameters and values obtained.
    #     """
    #     parameters = [1,1635000,1525000,1450000,1635090,1525090,1450090,5001,5004,5003,5002,4036,4035]
    #     if add_params:
    #         parameters += add_params
    #     try:
    #         if self.__protocol=='acoem':
    #             # warnings.warn("Not implemented.")
    #             data = self.get_values(parameters=parameters, verbosity=verbosity)
    #             if strict:
    #                 if 1 in parameters:
    #                     data[1] = data[1].strftime(format=f"%d/%m/%Y{sep}%H:%M:%S")
    #                 response = sep.join([str(data[k]) for k, v in data.items()])
    #                 data = {99: response}
    #         elif self.__protocol=='legacy':
    #             response = self.tcpip_comm(f"VI{self.__serial_id}99\r".encode(), verbosity=verbosity).decode()
    #             response = response.replace(", ", ",")
    #             if strict:
    #                 response = response.replace(',', sep)
    #                 data = {99: response}
    #             else:
    #                 response = response.split(',')
    #                 response = [response[0]] + [float(v) for v in response[1:]]
    #                 data = dict(zip(parameters, response))
    #         else:
    #             raise ValueError("Protocol not recognized.")
    #         return data
    #     except Exception as err:
    #         if self._log:
    #             self._logger.error(err)
    #         print(err)
    #         return dict()


    # def get_new_data(self, sep: str=",", save: bool=True, verbosity: int=0) -> str:
    #     """
    #     For the acoem format: Retrieve all self._instant_readings from (now - get_data_interval) until now.
    #     For the legacy format: Retrieve all self._instant_readings from current cursor.
        
    #     Args:
    #         sep (str, optional): Separator to use for output and file, respectively. Defaults to ",".
    #         save (bool, optional): Should data be saved to file? Defaults to True.
    #         verbosity (int, optional): _description_. Defaults to 0.

    #     Raises:
    #         Warning: _description_
    #         ValueError: _description_

    #     Returns:
    #         str: data retrieved from logger as decoded string, including line breaks.
    #     """
    #     try:
    #         dtm = time.strftime('%Y-%m-%d %H:%M:%S')
    #         print(f"{dtm} .get_new_data (name={self.__name}, save={save})")


    #         if self.__protocol=='acoem':
    #             if self.__get_data_interval is None:
    #                 raise ValueError("'get_data_interval' cannot be None.")
    #             tmp = []

    #             # define period ro retrieve and update state variable
    #             start = self.__start_datalog
    #             end = dt.datetime.now(dt.timezone.utc).replace(second=0, microsecond=0)
    #             self.__start_datalog = end + dt.timedelta(seconds=self.__data_log_interval)

    #             # retrieve data
    #             self.tcpip_comm_wait_for_line()            
    #             data = self.get_logged_data(start=start, end=end, verbosity=verbosity)
    #             if verbosity>0:
    #                 print(data)

    #             # prepare result
    #             for d in data:
    #                 values = [str(d.pop('dtm'))] + [str(value) for key, value in d.items()]
    #                 tmp.append(sep.join(values))
    #             data = '\n'.join(tmp) + '\n'

    #         elif self.__protocol=='legacy':
    #             data = self.tcpip_comm(f"***D\r".encode()).decode()
    #             data = data.replace('\r\n\n', '\r\n').replace(", ", ",").replace(",", sep)
    #         else:
    #             raise ValueError("Protocol not recognized.")
            
    #         if verbosity>0:
    #             print(data)

    #         if save:
    #             if self.__reporting_interval is None:
    #                 raise ValueError("'reporting_interval' cannot be None.")
                
    #             # generate the datafile name
    #             self.__datafile = os.path.join(self.__datadir, time.strftime("%Y"), time.strftime("%m"), time.strftime("%d"),
    #                                         "".join([self.__name, "-",
    #                                                 datetimebin.dtbin(self.__reporting_interval), ".dat"]))

    #             os.makedirs(os.path.dirname(self.__datafile), exist_ok=True)
    #             with open(self.__datafile, "at", encoding='utf8') as fh:
    #                 fh.write(data)
    #                 fh.close()

    #             if self.__staging:
    #                 self.stage_data_file()

    #         return data
        
    #     except Exception as err:
    #         if self._log:
    #             self._logger.error(err)
    #         print(err)
    #         return str()


    def _stage_and_store_data(self, current_time: datetime):
        """
        Write the collected data to a parquet file. 

        :param current_time: The current time to generate the file name.
        """
        file_name = f"Aurora3000-{current_time.strftime('%Y%m%dT%H')}.parquet"
        
        # write data to staging area
        self.data.write_parquet(os.path.join(self.staging_path, file_name))
        
        # write data to data area
        self.data.write_parquet(os.path.join(self.data_path, file_name))
        
        self.logger.info(f"{file_name} staged and stored.")

    def start(self):
        """
        Start the data collection process.
        """
        schedule.every(int(self.sampling_interval)).minutes.do(self.collect_readings)
        while True:
            schedule.run_pending()
            time.sleep(1)

if __name__ == "__main__":
    neph = Aurora3000(config_file='nrbdaq.yml')
    neph.start()
