import os
import schedule
import time
from nrbdaq.instr.thermo import Thermo49i
from nrbdaq.utils.sftp import SFTPClient
from nrbdaq.utils.utils import load_config, setup_logging, seconds_to_next_n_minutes

def main():
    # load configuation
    config = load_config(config_file='bucdaq.yml')

    # setup logging
    logfile = os.path.join(os.path.expanduser(config['root']), config['logging']['file'])
    logger = setup_logging(file=logfile)
    logger.info("== Start BUCDAQ =============", extra={'to_logfile': True})

    # setup sftp client
    sftp = SFTPClient(config=config)
    logger.debug(f"sftp.remote_path: {sftp.remote_path}")

    # setup Thermo 49i data acquisition and data transfer
    thermo49i = Thermo49i(config=config)
    thermo49i.setup_schedules()
    remote_path = os.path.join(sftp.remote_path, thermo49i.remote_path)
    sftp.setup_transfer_schedules(local_path=thermo49i.staging_path,
                                  remote_path=remote_path,
                                  interval=thermo49i.reporting_interval)

    # list all jobs
    logger.info(schedule.get_jobs(), extra={'to_logfile': True})

    # align start with a multiple-of-minute timestamp
    seconds_left = seconds_to_next_n_minutes(1)
    while seconds_left > 0:
        logger.info(f"Time remaining: {seconds_left:>0.1f} s", end="\r")
        dt = 1 #0.2
        time.sleep(dt)
        seconds_left -= dt
    logger.info("Beginning data acquisition and file transfer ...")

    # start jobs
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping data acquisition ...")
        # fidas.save_hourly()  # Save any remaining data on exit


if __name__ == "__main__":
    main()
