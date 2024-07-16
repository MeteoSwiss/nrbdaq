import serial
import time
import os
from datetime import datetime
import schedule
import argparse
import platform

# Minimal solution to collect data from AE31 and store to file. This script 
# will read from the serial port every 5 minutes and write the incoming data 
# to a text file. A new file will be started every 24 hours at midnight. 
# The script will also check if it's running on Windows or Linux and choose 
# a default port accordingly.
# Usage: Run this script from the command line, optionally providing the 
# serial port, baud rate, timeout, and target folder as arguments:
# 
# python3 ae31-mini.py --port /dev/ttyUSB0 --baudrate 9600 --timeout 1.0 --target /home/gaw/Documents/data

# 

def get_default_port() -> str:
    """
    Get the default serial port based on the operating system.

    :return: Default serial port as a string.
    """
    if platform.system() == 'Windows':
        return 'COM1'  # Default port for Windows
    else:
        return '/dev/ttyUSB0'  # Default port for Linux

def collect_data(port: str, baudrate: int, timeout: float, target: str):
    """
    Collect data from the serial port and write to a text file.

    :param port: The serial port to use.
    :param baudrate: The baud rate for the serial communication.
    :param timeout: The timeout value for the serial communication.
    :param target_folder: The folder to save the data files.
    """
    current_time = datetime.now()
    file_name = current_time.strftime("data_%Y%m%d.txt")
    file_path = os.path.join(target, file_name)

    try:
        with serial.Serial(port, baudrate, timeout=timeout) as ser:
            while True:
                # Check if a new day has started
                new_time = datetime.now()
                if new_time.date() != current_time.date():
                    file_name = new_time.strftime("data_%Y%m%d.txt")
                    file_path = os.path.join(target, file_name)
                    current_time = new_time

                if ser.in_waiting > 0:
                    raw_data = ser.readline().decode('ascii').strip()
                    timestamp = new_time.isoformat()

                    with open(file_path, 'a') as file:
                        file.write(f"{timestamp}, {raw_data}\n")

                time.sleep(300)  # Wait for 5 minutes

    except serial.SerialException as err:
        print(f"Serial communication error: {err}")
    except Exception as err:
        print(f"General error: {err}")

def main():
    """
    Main function to parse CLI arguments and start data collection.
    """
    parser = argparse.ArgumentParser(description="Serial port data collection.")
    parser.add_argument("--port", type=str, default=get_default_port(), help="Serial port (default: based on OS)")
    parser.add_argument("--baudrate", type=int, default=9600, help="Baud rate (default: 9600)")
    parser.add_argument("--timeout", type=float, default=1.0, help="Timeout in seconds (default: 1.0)")
    parser.add_argument("--target", type=str, default=".", help="Target folder for data files (default: current directory)")

    args = parser.parse_args()

    schedule.every().day.at("00:00").do(lambda: collect_data(args.port, args.baudrate, args.timeout, args.target_folder))

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()