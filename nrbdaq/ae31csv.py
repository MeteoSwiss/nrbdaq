"""
Basic function to open serial port, receive data every 5', append to CSV file, 
whereby a new CSV file called 'AE31_yyyymmdd.dat' is generated every day.
"""
import os
import schedule
import serial
import time
from datetime import datetime

def retrieve_and_save_data(serial_port: str, data_path: str):
    # open serial port
    with serial.Serial(serial_port, 9600, 8, 'N', 1, timeout=270) as ser:
        data_received = ser.readline() #.decode()
        dtm = datetime.now().isoformat()
        print(f"{dtm}: {data_received[:80]} ..."),

        # open file for the day and get ready to write to it
        dte_today = datetime.now().strftime('%Y%m%d')
        file_path = os.path.join(data_path, f"AE31_{dte_today}.csv")
        if os.path.exists(file_path):
            mode = 'a'
        else:
            mode = 'w'
            print(f"# Reading data and writing to AE31_{dte_today}.csv")
        
        with open(file=file_path, mode=mode) as fh:
            fh.write(f"{dtm}, {data_received}")
            fh.close()

        ser.close()

if __name__ == "__main__":
    serial_port = '/dev/ttyUSB0'
    data_path = os.path.join('data', 'ae31')

    schedule.every(5).minutes.do(retrieve_and_save_data, serial_port, data_path)

    while True:
        schedule.run_pending()
        time.sleep(1)