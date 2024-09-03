import os
import schedule
import time
from nrbdaq.instr.ae31 import AE31
import nrbdaq.instr.avo as avo
from nrbdaq.instr.thermo import Thermo49i
from nrbdaq.utils.sftp import SFTPClient
from nrbdaq.utils.utils import load_config, setup_logging


def main():
    # load configuation
    config = load_config(config_file='nrbdaq.yml')

    # setup logging
    logfile = os.path.join(os.path.expanduser(config['root']), config['logging']['file'])
    logger = setup_logging(file=logfile)
    logger.info("== Start NRBDAQ =============")

    # setup sftp client
    sftp = SFTPClient(config=config)

    # setup AE31 data acquisition and data transfer
    ae31 = AE31(config=config)
    remote_path = os.path.join(sftp.remote_path, ae31.remote_path)
    sftp.setup_transfer_schedule(local_path=ae31.staging_path, 
                                 remote_path=remote_path, 
                                 transfer_interval=ae31.reporting_interval)  

    # setup Nairobi AVO data download, staging and transfer
    data_path = os.path.join(os.path.expanduser(config['root']), config['AVO']['data'])
    staging_path = os.path.join(os.path.expanduser(config['root']), config['AVO']['staging'])
    remote_path = os.path.join(sftp.remote_path, config['AVO']['remote_path'])
    download_interval = config['AVO']['download_interval']
    hours = [f"{download_interval*n:02}:00" for n in range(23) if download_interval*n <= 23]
    for hr in hours:
        schedule.every().day.at(hr).do(avo.download_multiple,
                                       urls={'url_nairobi': config['AVO']['urls']['url_nairobi']}, 
                                       file_path=data_path, 
                                       staging=staging_path)
    sftp.setup_transfer_schedule(local_path=staging_path, 
                                 remote_path=remote_path, 
                                 transfer_interval=download_interval)  

    # setup Thermo 49i data acquisition and data transfer
    thermo49i = Thermo49i(config=config)
    thermo49i.setup_schedules()
    remote_path = os.path.join(sftp.remote_path, thermo49i.remote_path)
    sftp.setup_transfer_schedule(local_path=thermo49i.staging_path, 
                                 remote_path=remote_path, 
                                 transfer_interval=thermo49i.reporting_interval)  

    # start data acquisition, staging and transfer
    logger.info(schedule.get_jobs())

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
