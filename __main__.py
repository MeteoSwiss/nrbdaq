import schedule
import time
from nrbdaq.instr.ae31 import AE31
from nrbdaq.utils.sftp import SFTPClient

def main():
    ae31 = AE31(config_file='nrbdaq.cfg')
    sftp = SFTPClient(config_file='nrbdaq.cfg')

    # Setup schedules and start the data collection process.
    schedule.every(5).minutes.at(":00").do(ae31.collect_readings_hourly)
    schedule.every(60).minutes.at("00:05").do(ae31.stage_data)
    schedule.every(60).minutes.at("00:10").do(sftp.xfer_r)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__=="__main__":
    main()