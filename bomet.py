import os
import schedule
import time
import nrbdaq.instr.avo as avo
from nrbdaq.instr.thermo import Thermo49i
# from nrbdaq.utils.sftp import SFTPClient
from nrbdaq.utils.utils import load_config, setup_logging, seconds_to_next_n_minutes


def main():
    # load configuation
    config = load_config(config_file='bomet.yml')

    # setup logging
    logfile = os.path.join(os.path.expanduser(config['root']), config['logging']['file'])
    logger = setup_logging(file=logfile)
    logger.info("== Start BOMET DAQ =============")

    # # setup sftp client
    # sftp = SFTPClient(config=config)
    # logger.debug(f"sftp.remote_path: {sftp.remote_path}")

    # setup Nairobi AVO data download, staging and transfer
    data_path = os.path.join(os.path.expanduser(config['root']), config['data'], config['AVO']['data_path'])
    staging_path = os.path.join(os.path.expanduser(config['root']), config['staging'], config['AVO']['staging_path'])
    # remote_path = os.path.join(sftp.remote_path, config['AVO']['remote_path'])
    download_interval = config['AVO']['download_interval']
    hours = [f"{download_interval*n:02}:00" for n in range(23) if download_interval*n <= 23]
    for hr in hours:
        schedule.every(1).day.at(hr).do(avo.download_multiple,
                                       urls={'url_nairobi': config['AVO']['urls']['url_nairobi'],
                                             'url_mogogosiek': config['AVO']['urls']['url_mogogosiek'],
                                             'url_bomet': config['AVO']['urls']['url_bomet']},
                                       file_path=data_path,
                                       staging=staging_path)
    # sftp.setup_transfer_schedules(local_path=staging_path,
    #                               remote_path=remote_path,
    #                               interval=download_interval)

    # setup Thermo 49i data acquisition and data transfer
    thermo49i = Thermo49i(config=config)
    thermo49i.setup_schedules()
    # remote_path = os.path.join(sftp.remote_path, thermo49i.remote_path)
    # sftp.setup_transfer_schedules(local_path=thermo49i.staging_path,
    #                               remote_path=remote_path,
    #                               interval=thermo49i.reporting_interval)

    # list all jobs
    logger.info(schedule.get_jobs())

    # align start with a multiple-of-minute timestamp
    seconds_left = seconds_to_next_n_minutes(1)
    while seconds_left > 0:
        print(f"Time remaining: {seconds_left:0.1f} s", end="\r")
        dt = 0.2
        time.sleep(dt)
        seconds_left -= dt
    logger.info("Beginning data acquisition and file transfer ...")

    # start jobs
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
