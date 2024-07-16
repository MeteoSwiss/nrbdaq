import configparser
import logging
import os
import time
from datetime import datetime
from typing import Optional
from nrbdaq.utils.utils import setup_logger, load_config
import polars as pl
import schedule
import serial


class Aurora3000:
    def __init__(self, config_file: str = 'nrbdaq.cfg'):
        """
        Initialize the AE31 instrument class with parameters from a configuration file.

        :param config_file: Path to the configuration file.
        """
        try:
            self.config = load_config(config_file)
            
            self.logger = setup_logger(name=__name__,
                          file=self.config['logging']['file'], 
                          level_console=self.config['logging']['level_console'], 
                          level_file=self.config['logging']['level_file'])

            # self.simulate = self.config['app']['simulate']
            self.simulate = False

            self.staging_root = os.path.expanduser(self.config['staging']['root'])
            os.makedirs(self.staging_root, exist_ok=True)
            self.data_root = os.path.expanduser(self.config['data']['root'])
            os.makedirs(self.data_root, exist_ok=True)
            # self.data = pl.DataFrame({"timestamp": [], "data": []})
            self.data = pl.DataFrame({"timestamp": [str()], "data": [str()]})
            
            self.current_hour = datetime.now().hour

            self.port = self.config['Aurora3000']['serial_port']
            self.baudrate = int(self.config['Aurora3000']['serial_baudrate'])
            self.timeout = float(self.config['Aurora3000']['serial_timeout'])
            if self.simulate:
                self.read_interval = 1
            else:
                self.read_interval = self.config['Aurora3000']['read_interval']
                self.serial_conn = serial.Serial(self.port, self.baudrate, timeout=self.timeout)

        except serial.SerialException as err:
            self.logger.error(f"Serial communication error: {err}")
            pass
        except Exception as err:
            self.logger.error(f"General error: {err}")
            pass

    def collect_readings(self):
        """
        Collect new readings from the Aurora3000 instrument and store them in hourly parquet files.
        """
        try:
            if self.simulate:
                raw_data = f"this,is,a,test"
            elif self.serial_conn.in_waiting == 0:
                raw_data = self.read_new_data()
            # elif self.serial_conn.in_waiting > 0:
            #     raw_data = self.serial_conn.readline().decode('ascii').strip() 
            current_time = datetime.now()
            timestamp = current_time.isoformat()

            if raw_data:
                # Check for duplicates
                # if not self.data.filter(pl.col("data") == raw_data).is_empty():
                if self.data.filter(pl.col("data") == raw_data).is_empty():
                    new_row = pl.DataFrame({"timestamp": [timestamp], "data": [raw_data]})
                    self.data = pl.concat([self.data, new_row], how='diagonal')

            # Check if we need to write the data to a new file
            if self.config['app']['simulate'] or current_time.hour != self.current_hour:
                self.stage_and_store_data(current_time)
                self.data = pl.DataFrame({"timestamp": [str()], "data": [str()]})
                self.current_hour = current_time.hour

        except serial.SerialException as err:
            self.logger.error(f"Serial communication error: {err}")
            pass
        except Exception as err:
            self.logger.error(f"General error: {err}")
            pass

    def read_new_data(self, sep: str=',') -> list[str]:
        msg = f"***D\r".encode()
        self.serial_conn.write(msg)
        time.sleep(0.2)
        data = self.serial_conn.readline().decode('ascii').strip() 
        data = data.replace('\r\n\n', '\r\n').replace(", ", ",").replace(",", sep)
        return data

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
    #     For the acoem format: Retrieve all readings from (now - get_data_interval) until now.
    #     For the legacy format: Retrieve all readings from current cursor.
        
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


    def stage_and_store_data(self, current_time: datetime):
        """
        Write the collected data to a parquet file. 

        :param current_time: The current time to generate the file name.
        """
        if self.simulate:
            file_name = f"Aurora3000_{current_time.strftime('%Y%m%dT%H%M')}.parquet"
        else:
            file_name = f"Aurora3000_{current_time.strftime('%Y%m%dT%H')}.parquet"
        
        # write data to staging area
        self.data.write_parquet(os.path.join(self.staging_root, file_name))
        
        # write data to data area
        self.data.write_parquet(os.path.join(self.data_root, file_name))
        
        self.logger.info(f"{file_name} staged and stored.")

    def start(self):
        """
        Start the data collection process.
        """
        schedule.every(int(self.read_interval)).minutes.do(self.collect_readings)
        while True:
            schedule.run_pending()
            time.sleep(1)

if __name__ == "__main__":
    neph = Aurora3000(config_file='nrbdaq.cfg')
    neph.start()
