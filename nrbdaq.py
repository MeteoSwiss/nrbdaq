import os
import schedule
import time
from nrbdaq.instr.ae31 import AE31
import nrbdaq.instr.avo as avo
from nrbdaq.utils.sftp import SFTPClient
from nrbdaq.utils.utils import load_config, setup_logging


def main():
    # load configuation
    config = load_config(config_file='nrbdaq.yaml')

    # setup logging
    logger = setup_logging(os.path.join(os.path.expanduser(config['root']), config['logging']['file']))

    # instantiate instrument(s)
    ae31 = AE31(config=config)

    # setup AVO data download for Nairobi
    url = config['AVO']['urls']['url_nairobi']
    file_path=os.path.join(os.path.expanduser(config['root']), config['AVO']['data'])
    staging=os.path.join(os.path.expanduser(config['root']), config['AVO']['staging'])
    schedule.every(6).hours.at(':00').do(avo.download_multiple, 
                                         urls={'url_nairobi': url}, 
                                         file_path=file_path, 
                                         staging=staging)

    # setup sftp client
    sftp = SFTPClient(config=config)

    # start data acquisition, staging and transfer
    logger.info("Start NRBDAQ")
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
