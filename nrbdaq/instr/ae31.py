import configparser
import logging
import os
import time
from datetime import datetime
from typing import Optional

import polars as pl
import schedule
import serial


class AE31:
    def __init__(self, config_file: str = 'nrbdaq.cfg'):
        """
        Initialize the AE31 instrument class with parameters from a configuration file.

        :param config_file: Path to the configuration file.
        """
        self.config = self.load_config(config_file)
        self.port = self.config['serial']['port']
        self.baudrate = int(self.config['serial']['baudrate'])
        self.timeout = float(self.config['serial']['timeout'])
        self.target_folder = self.config['output']['target_folder']
        
        self.setup_logger()
        
        self.serial_conn = serial.Serial(self.port, self.baudrate, timeout=self.timeout)
        self.data = pl.DataFrame({"timestamp": [], "data": []})
        self.current_hour = datetime.now().hour

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
        Setup logger for the AE31 class.
        """
        log_file = self.config['logging']['log_file']
        log_level = self.config['logging']['log_level'].upper()

        logging.basicConfig(
            filename=log_file,
            level=getattr(logging, log_level, logging.ERROR),
            format='%(asctime)s:%(levelname)s:%(message)s'
        )

    def collect_readings(self):
        """
        Collect readings from the AE31 instrument and store them in hourly parquet files.
        """
        try:
            if self.serial_conn.in_waiting > 0:
                raw_data = self.serial_conn.readline().decode('ascii').strip()
                current_time = datetime.now()
                timestamp = current_time.isoformat()

                if raw_data:
                    # Check for duplicates
                    if not self.data.filter(pl.col("data") == raw_data).is_empty():
                        new_row = pl.DataFrame({"timestamp": [timestamp], "data": [raw_data]})
                        self.data = pl.concat([self.data, new_row])

                # Check if we need to write the data to a new file
                if current_time.hour != self.current_hour:
                    self.write_to_parquet(current_time)
                    self.data = pl.DataFrame({"timestamp": [], "data": []})
                    self.current_hour = current_time.hour

        except serial.SerialException as e:
            logging.error(f"Serial communication error: {e}")
        except Exception as e:
            logging.error(f"General error: {e}")

    def write_to_parquet(self, current_time: datetime):
        """
        Write the collected data to a parquet file.

        :param current_time: The current time to generate the file name.
        """
        file_name = f"AE31_{current_time.strftime('%Y%m%dT%H')}.parquet"
        file_path = os.path.join(self.target_folder, file_name)
        self.data.write_parquet(file_path)

    def start(self):
        """
        Start the data collection process.
        """
        schedule.every(5).minutes.do(self.collect_readings)
        while True:
            schedule.run_pending()
            time.sleep(1)

if __name__ == "__main__":
    ae31 = AE31(config_file='nrbdaq.cfg')
    ae31.start()
