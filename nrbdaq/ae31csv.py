"""
Basic function to open serial port, receive data every 5', append to CSV file, 
whereby a new CSV file called 'AE31_yyyymmdd.dat' is generated every day.
"""
import serial
from datetime import datetime
# open serial port
ser = serial.Serial('/dev/ttyUSB0', 9600, 8, 'N', 1, timeout=360)

# open file for the day and get ready to write to it
dte_today = datetime.now().strftime('%Y%m%d')
file_data = open(f"nrbdaq/data/ae31/AE31_{dte_today}.csv", 'w')
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
            file_data = open(f"nrbdaq/data/ae31/AE31_{dte_today}.csv", 'w')
            print(f"# Reading data and writing to AE31_{dte_today}.csv")
            file_data.write(f"{dtm}, {data_received}")
