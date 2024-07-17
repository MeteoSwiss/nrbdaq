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

class AE31:
    def __init__(self, config_file: str = 'nrbdaq.cfg'):
        """
        Initialize the AE31 instrument class with parameters from a configuration file.

        :param config_file: Path to the configuration file.
        """
        try:
            # self.logger = logging.getLogger(__name__)

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

            self.port = self.config['AE31']['serial_port']
            self.baudrate = int(self.config['AE31']['serial_baudrate'])
            self.timeout = float(self.config['AE31']['serial_timeout'])
            if self.simulate:
                self.read_interval = 1
            else:
                self.read_interval = self.config['AE31']['read_interval']
                self.serial_conn = serial.Serial(self.port, self.baudrate, timeout=self.timeout)
                self.serial_conn.close()

        except serial.SerialException as err:
            self.logger.error(f"Serial communication error: {err}")
            pass
        except Exception as err:
            self.logger.error(f"General error: {err}")
            pass

    
    def collect_readings_daily_basic(self):
        """
        Basic function to open serial port, receive data every 5', append to CSV file, 
        whereby a new CSV file called 'AE31_yyyymmdd.dat' is generated every day.
        """
        # open serial port
        ser = serial.Serial('/dev/ttyUSB0', 9600, 8, 'N', 1, timeout=360)
        
        # open file for the day and get ready to write to it
        dte_today = datetime.now().strftime('%Y%m%d')
        file_data = open(f"AE31_{dte_today}.csv", 'w')
        print(f"# Reading data and writing to AE31_{dte_today}.csv")
        data_received = " "

        # Listen for the input, exit if nothing received in timeout period
        while True:
            while data_received:
                data_received = ser.readline().decode()
                print(f"{data_received[:80]} ..."),
                
                dtm = datetime.now().isoformat()
                if dte_today==datetime.now().strftime('%Y%m%d'):
                    file_data.write(f"{dtm}, {data_received}")
                else:
                    file_data.close()
                    dte_today = datetime.now().strftime('%Y%m%d')
                    file_data = open(f"AE31_{dte_today}.csv", 'w')
                    print(f"# Reading data and writing to AE31_{dte_today}.csv")
                    file_data.write(f"{dtm}, {data_received}")

    
    def collect_readings_hourly_basic(self):
        """
        Basic function to open serial port, receive data every 5', append to CSV file, 
        whereby a new CSV file called 'AE31_yyyymmddTHH.dat' is generated every hour.
        """
        # open serial port
        ser = serial.Serial('/dev/ttyUSB0', 9600, 8, 'N', 1, timeout=360)
        
        # open file for the day and get ready to write to it
        dtm_this_hour = datetime.now().strftime('%Y%m%dT%H')
        file_data = open(f"/home/gaw/Documents/data/ae31/AE31_{dtm_this_hour}.csv", 'w')
        print(f"# Reading data and writing to AE31_{dtm_this_hour}.csv")
        data_received = " "

        # Listen for the input, exit if nothing received in timeout period
        while True:
            while data_received:
                data_received = ser.readline().decode()
                print(f"{data_received[:80]} ..."),
                
                dtm = datetime.now().isoformat()
                if dtm_this_hour==datetime.now().strftime('%Y%m%dT%H'):
                    file_data.write(f"{dtm}, {data_received}")
                else:
                    file_data.close()
                    dtm_this_hour = datetime.now().strftime('%Y%m%dT%H')
                    file_data = open(f"/home/gaw/Documents/data/AE31_{dtm_this_hour}.csv", 'w')
                    print(f"# Reading data and writing to AE31_{dtm_this_hour}.csv")
                    file_data.write(f"{dtm}, {data_received}")


    def collect_readings_hourly(self):
        """
        Basic function to open serial port, receive data every 5', append to CSV file, 
        whereby a new CSV file called 'AE31_yyyymmddTHH.dat' is generated every hour.
        """
        # open serial port
        ser = serial.Serial('/dev/ttyUSB0', 9600, 8, 'N', 1, timeout=360)
        
        # open file for the hour and get ready to write to it
        root = os.path.expanduser("~/Documents/data")
        dtm = datetime.now()
        dtm_this_hour = dtm.strftime('%Y%m%dT%H')
        yyyy = dtm.strftime("%Y")
        mm = dtm.strftime("%m")
        dd = dtm.strftime("%d")
        
        file_data = open(f"/home/gaw/Documents/data/AE31_{dtm_this_hour}.csv", 'w')
        print(f"# Reading data and writing to AE31_{dtm_this_hour}.csv")
        data_received = " "

        # Listen for the input, exit if nothing received in timeout period
        while True:
            while data_received:
                data_received = ser.readline().decode()
                print(f"{data_received[:80]} ..."),
                
                dtm = datetime.now().isoformat()
                if dtm_this_hour==datetime.now().strftime('%Y%m%dT%H'):
                    file_data.write(f"{dtm}, {data_received}")
                else:
                    file_data.close()
                    dtm_this_hour = datetime.now().strftime('%Y%m%dT%H')
                    file_data = open(f"/home/gaw/Documents/data/AE31_{dtm_this_hour}.csv", 'w')
                    print(f"# Reading data and writing to AE31_{dtm_this_hour}.csv")
                    file_data.write(f"{dtm}, {data_received}")

    
    def collect_readings(self, echo: bool=False):
        """
        Collect readings from the AE31 instrument and store them in hourly parquet files.
        """
        try:
            if self.serial_conn.closed:
                self.serial_conn.open()

            raw_data = str()
            if self.simulate:
                raw_data = f"this,is,a,test"
            elif self.serial_conn.in_waiting > 0:
                raw_data = self.serial_conn.readline().decode('ascii').strip()     
                if echo:
                    print(raw_data)
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

    def stage_and_store_data(self, current_time: datetime):
        """
        Write the collected data to a parquet file. 

        :param current_time: The current time to generate the file name.
        """
        if self.simulate:
            file_name = f"AE31_{current_time.strftime('%Y%m%dT%H%M')}.parquet"
        else:
            file_name = f"AE31_{current_time.strftime('%Y%m%dT%H')}.parquet"
        
        # write data to staging area
        self.data.write_parquet(os.path.join(self.staging_root, file_name))
        
        # write data to data area
        self.data.write_parquet(os.path.join(self.data_root, file_name))
        
        self.logger.info(f"{file_name} staged and stored.")

    def plot_data(self, filepath: str, save: bool=True):
        df = pl.read_parquet(filepath)


    def start(self):
        """
        Start the data collection process.
        """
        schedule.every(int(self.read_interval)).minutes.do(self.collect_readings)
        while True:
            schedule.run_pending()
            time.sleep(1)

if __name__ == "__main__":
    ae31 = AE31(config_file='nrbdaq.cfg')
    ae31.start()
