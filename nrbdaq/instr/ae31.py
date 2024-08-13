import os
import colorama
import logging
from datetime import datetime, timedelta
import polars as pl
# from typing import Optional
# from nrbdaq.utils.utils import setup_logger, load_config
import schedule
import shutil
import serial

# 14.9.3  Data File Format - Seven wavelength Instruments 
# The AE-3 series seven wavelength Aethalometers measure optical 
# absorbance at seven optical wavelengths from 370 to 950 nm.  The data 
# are reported on a single line written to disk as follows: 
# • Expanded Data Format:  “date”, “time”, UV [370 nm] result,  
# Blue [470 nm] result, Green [520 nm] result, Yellow [590 nm] 
# result, Red [660 nm] result, IR1 [880 nm, “standard BC”] result, 
# IR2 [950 nm] result,  air flow (LPM), bypass fraction, 
# and then the following columns of data repeated for the seven 
# measurement wavelengths: 
# sensing zero signal, sensing beam signal, reference zero signal, 
# reference beam signal, optical attenuation, air flow (LPM), bypass 
# fraction.    
# The ‘air flow’ and ‘bypass fraction’ columns are repeated to allow for 
# easy visual identification of the separation between the seven sets of 
# data columns. 
# A typical line in the data file might look like: 
# "24-jul-00","16:40", 610 , 604 , 605 , 612 , 617 , 611 , 641 , 3.131 ,-
# .9812 ,-.9814 , 1.1881 , 1.8384 , 1 , 6.4 , 2.704 ,-.9812 ,-.9814 , 4.2483 
# , 2.7373 , 1 , 6.4 , 2.45 ,-.9812 ,-.9814 , 2.1716 , 1.9438 , 1 , 6.4 , 2.232 
# ,-.9812 ,-.9814 , 2.854 , 3.5259 , 1 , 6.4 , 1.957 ,-.9812 ,-.9814 , 3.3428 
# , 2.596 , 1 , 6.4 , 1.452 ,-.9812 ,-.9814 , 4.6719 , 3.3935 , 1 , 6.4 , 1.396 
# ,-.9812 ,-.9814 , 2.705 , 2.438 , 1 , 6.4  

class AE31:
    def __init__(self, config: dict):
        """Initialize the AE31 instrument class with parameters from a configuration file.

        Args:
            config (dict): general configuration
        """
        colorama.init(autoreset=True)

        try:
            # configure logging
            _logger = f"{os.path.basename(config['logging']['file'])}".split('.')[0]
            self.logger = logging.getLogger(f"{_logger}.{__name__}")
            self.logger.info("Initialize AE31")
            
            # configure serial port
            self.serial_port = config['AE31']['serial_port']
            self.serial_timeout = config['AE31']['serial_timeout']
            
            # configure data storage and reporting interval (which determines in what chunks data are persisted)
            self.data_path = os.path.expanduser(config['AE31']['data'])
            os.makedirs(self.data_path, exist_ok=True)
            self.reporting_interval = config['AE31']['reporting_interval']
            
            # configure data archive
            self.archive_path = os.path.expanduser(config['AE31']['archive'])
            os.makedirs(self.archive_path, exist_ok=True)
            
            # configure data transfer
            self.staging_path = os.path.expanduser(config['AE31']['staging'])
            os.makedirs(self.staging_path, exist_ok=True)

            self.host = config['sftp']['host']
            self.usr = config['sftp']['usr']
            self.key = os.path.expanduser(config['sftp']['key'])
            
            # initialize data response and datetime stamp           
            self.data = str()
            self.dtm = None

            # configure data collection, saving and staging
            self.sampling_interval = config['AE31']['sampling_interval']
            schedule.every(int(self.sampling_interval)).minutes.at(':00').do(self.serial_read_data)
            schedule.every(int(self.sampling_interval)).minutes.at(':01').do(self.save_data)
            if self.reporting_interval=='daily':
                schedule.every(1).days.at('00:01:00').do(self.stage_data)
            elif self.reporting_interval=='hourly':
                schedule.every(1).hours.at('00:01').do(self.stage_data)
            else:
                raise ValueError('reporting_interval must be one of daily|hourly.')
        except Exception as err:
            self.logger.error(err)
            pass

    
    def serial_read_data(self) -> None:
        try:
            with serial.Serial(self.serial_port, 9600, 8, 'N', 1, int(self.serial_timeout)) as ser:
                self.dtm = datetime.now().isoformat(timespec='seconds')
                self.data = ser.readline().decode('ascii').strip()
                self.logger.info(f"{self.dtm}: {self.data[:80]} ..."),
            return

        except serial.SerialException as err:
            self.logger.error(f"SerialException: {err}")
            pass
        except Exception as err:
            self.logger.error(err)


    def save_data(self):
        try:
            if self.data:
                if self.reporting_interval=='daily':
                    timestamp = datetime.now().strftime('%Y%m%d')
                elif self.reporting_interval=='hourly':
                    timestamp = datetime.now().strftime('%Y%m%d%H')
                else:
                    raise ValueError('reporting_interval must be one of daily|hourly.')
                file = os.path.join(self.data_path, f"AE31_{timestamp}.csv")
                if os.path.exists(file):
                    mode = 'a'
                else:
                    mode = 'w'
                    self.logger.info(f"# Reading data and writing to {self.data_path}/AE31_{timestamp}.csv")
                
                # open file and write to it
                with open(file=file, mode=mode) as fh:
                    fh.write(f"{self.dtm}, {self.data}\n")

        except Exception as err:
            self.logger.error(err)


    def stage_data(self):
        """Copy final data file to the staging area. Establish the timestamp of the previous (now complete) file, then copy it to the staging area."""
        if self.reporting_interval=='daily':
            timestamp = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        elif self.reporting_interval=='hourly':
            timestamp = (datetime.now() - timedelta(hours=1)).strftime('%Y%m%d%H')
        else:
            raise ValueError('reporting_interval must be one of daily|hourly.')
        file = f"AE31_{timestamp}.csv"

        try:
            if os.path.exists(file):
                shutil.copyfile(src=os.path.join(self.data_path, file), 
                                dst=os.path.join(self.staging_path, file))
        except Exception as err:
            self.logger.error(err)


    def compile_data(self, remove_duplicates: bool=True) -> pl.DataFrame:
        """Compile data files and save as .parquet

        Returns:
            pl.DataFrame: compiled data sets
        """
        df = pl.DataFrame()

        for root, dirs, files in os.walk(self.data_path):
            for file in files:
                if df.is_empty():
                    df = pl.read_csv(os.path.join(root, file), has_header=False)
                else:
                    try:
                        df = pl.concat([df, pl.read_csv(os.path.join(root, file), has_header=False)], how="diagonal")
                    except:
                        self.logger.error(f"{file} could not be appended.")
                        pass
        if remove_duplicates:
            df = df.unique()
        
        df.sort()
        df.write_parquet(os.path.join(self.archive_path, 'ae31_nrb.parquet'))


    def plot_data(self, filepath: str, save: bool=True):
        self.logger.warning("Not implemented.")


if __name__ == "__main__":
    pass