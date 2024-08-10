import schedule
import time
from nrbdaq.instr.ae31 import AE31
from nrbdaq.utils.sftp import SFTPClient
from nrbdaq.utils.utils import load_config, setup_logging

def main():
    # load configuation
    config = load_config(config_file='nrbdaq.cfg')

    # setup logging
    logger = setup_logging(config['logging']['file'])

    # instantiate instrument(s)
    ae31 = AE31(config=config)

    # setup sftp client
    sftp = SFTPClient(config=config)

    # start data acquisition, staging and transfer
    logger.info("Start NRBDAQ")
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
    


