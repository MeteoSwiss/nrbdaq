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
    try:
        # open serial port
        with serial.Serial(serial_port, 9600, 8, 'N', 1, timeout=270) as ser:
            # configure file for the day
            dte_today = datetime.now().strftime('%Y%m%d')
            file = os.path.join(data_path, f"AE31_{dte_today}.csv")
            if os.path.exists(file):
                mode = 'a'
            else:
                mode = 'w'
                print(f"# Reading data and writing to AE31_{dte_today}.csv")
            
            dtm = datetime.now().isoformat(timespec='seconds')
            data_received = ser.readline().decode('ascii').strip()
            print(f"{dtm}: {data_received[:80]} ..."),

            # open file and write to it
            with open(file=file, mode=mode) as fh:
                fh.write(f"{dtm}, {data_received}\n")

    except serial.SerialException as err:
        print(f"Serial communication error: {err}")
    except Exception as err:
        print(f"General error: {err}")

if __name__ == "__main__":
    serial_port = '/dev/ttyUSB0'
    data_path = os.path.join(os.getcwd(), 'data', 'ae31')
    os.makedirs(data_path, exist_ok=True)

    schedule.every(1).minutes.do(retrieve_and_save_data, serial_port, data_path)

    while True:
        schedule.run_pending()
        time.sleep(1)