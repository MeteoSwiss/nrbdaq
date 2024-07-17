import serial
import datetime

# Read & print/write data
ser = serial.Serial('/dev/ttyUSB0',9600, 8, 'N', 1, timeout = 360)
# Open file and write to it
dte_today = datetime.datetime.now().strftime('%Y%m%d')

file_data = open(f"AE31_{dte_today}.csv", 'w')
print(f"Reading data and writing to AE31_{dte_today}.csv")
# Listen for the input, exit if nothing received in timeout period
output = " "
while True:
  while output != "":
    output = ser.readline().decode()
    print(output),
    
    dtm = datetime.datetime.now().isoformat()
    if dte_today==datetime.datetime.now().strftime('%Y%m%d'):
        file_data.write(f"{dtm}, {output}")
    else:
        file_data.close()
        dte_today = datetime.datetime.now().strftime('%Y%m%d')
        file_data = open(f"AE31_{dte_today}.csv", 'w')
        print(f"Reading data and writing to AE31_{dte_today}.csv")
        file_data.write(f"{dtm}, {output}")
        
  output = " "
  
  

# Close file
print("Stopped writing to ae31.csv")
file_data.close()