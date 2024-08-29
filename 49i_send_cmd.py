import os
import argparse
from nrbdaq.instr.thermo import Thermo49i
from nrbdaq.utils.utils import load_config, setup_logging

def main():
    config = load_config(config_file='nrbdaq.yaml')
    tei49i = Thermo49i(config=config)

    # setup logging
    logfile = os.path.join(os.path.expanduser(config['root']), config['logging']['file'])
    logger = setup_logging(file=logfile)

    parser = argparse.ArgumentParser(
        description='Send command to Thermo 49i and receive response.')
    parser.add_argument('cmd', default='o3', type=str, action="store_true",
                        help='Command to be sent (default: o3)')
    args = parser.parse_args()

    response = tei49i.send_command(cmd=args.cmd)
    logger.info(f"sent: {args.cmd}; rcvd: {response}")


if __name__ == "__main__":
    main()