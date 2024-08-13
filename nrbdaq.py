import os
import schedule
import time
from nrbdaq.instr.ae31 import AE31
import nrbdaq.instr.avo as avo
from nrbdaq.utils.sftp import SFTPClient
from nrbdaq.utils.utils import load_config, setup_logging


def main():
    # load configuation
    config = load_config(config_file='nrbdaq.cfg')

    # setup logging
    logger = setup_logging(config['logging']['file'])

    # instantiate instrument(s)
    ae31 = AE31(config=config)

    # setup AVO data download (NB: we treat all 3 AVOs, not only Nairobi)
    urls = dict(config['AVO'])
    file_path = os.path.expanduser(config['AVO']['data'])
    schedule.every(1).days.at('00:00:00').do(avo.get_data_all, urls, file_path)

    # setup sftp client
    sftp = SFTPClient(config=config)

    # start data acquisition, staging and transfer
    logger.info("Start NRBDAQ")
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
