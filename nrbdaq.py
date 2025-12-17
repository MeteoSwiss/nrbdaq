import time
from pathlib import Path, PurePosixPath

import schedule

import nrbdaq.instr.avo as avo
from nrbdaq.instr.ae31 import AE31
from nrbdaq.instr.aurora3000 import Aurora3000
from nrbdaq.instr.fidas import FIDAS
from nrbdaq.instr.thermo import Thermo49i
from nrbdaq.utils.s3fsc import S3FSC
from nrbdaq.utils.sftp import SFTPClient
from nrbdaq.utils.utils import (load_config, seconds_to_next_n_minutes,
                                setup_logging)


def main():
    # load configuation
    config = load_config(config_file='nrbdaq.yml')
    if not config:
        raise RuntimeError("Config loaded empty; check path and YAML syntax.")
    
    # setup logging
    logfile = Path(config['root']).expanduser() / config['logging']['file']
    logger = setup_logging(file=str(logfile))
    logger.info("== Start NRBDAQ =============", extra={'to_logfile': True})

    # decide on file transfer mechanism
    s3fsc = None
    sftp = None

    # Prefer S3 when config contains an 's3' section
    if config.get("s3"):
        # You can control these via mkndaq.yml's s3.* or override here if needed
        s3fsc = S3FSC(
            config=config,
            use_proxies=bool(config["s3"].get("use_proxies", True)),
            addressing_style=config["s3"].get("addressing_style", "path"),
            verify=config["s3"].get("verify", True),
            default_prefix=config["s3"].get("default_prefix", ""),
        )
    if SFTPClient and config.get("sftp"):
        # Optional fallback if S3 is not configured
        sftp = SFTPClient(config=config)
    else:
        raise RuntimeError("Neither S3 nor SFTP is configured in mkndaq.yml")


    # # setup sftp client
    # sftp = SFTPClient(config=config)
    # logger.debug(f"sftp.remote_path: {sftp.remote_path}")

    try:
        # setup FIDAS
        if config.get('fidas'):
            fidas = FIDAS(config=config)
            fidas.connect_udp()
            fidas.setup_schedules()
            if s3fsc:
                s3fsc.setup_transfer_schedules(
                    local_path=str(fidas.staging_path),
                    key_prefix=fidas.remote_path,
                    interval=fidas.reporting_interval,
                    delay_transfer=3,
                    remove_on_success=False,
                )
            if sftp:
                remote_path = (PurePosixPath(sftp.remote_path) / fidas.remote_path).as_posix()
                sftp.setup_transfer_schedules(local_path=fidas.staging_path,
                                            remote_path=remote_path,
                                            interval=fidas.reporting_interval)

        # setup AE31 data acquisition and data transfer
        if config.get('ae31'):
            ae31 = AE31(config=config)
            ae31.setup_schedules()
            
            if s3fsc:
                s3fsc.setup_transfer_schedules(
                    local_path=str(ae31.staging_path),
                    key_prefix=ae31.remote_path,
                    interval=ae31.reporting_interval,
                    delay_transfer=3,
                    remove_on_success=False,
                )
            if sftp:
                remote_path = (PurePosixPath(sftp.remote_path) / ae31.remote_path).as_posix()
                sftp.setup_transfer_schedules(local_path=ae31.staging_path,
                                            remote_path=remote_path,
                                            interval=ae31.reporting_interval)

        # setup HMP110 data acquisition and data transfer
        if config.get('hmp110-inlet', None):
            from nrbdaq.instr.vaisala import HMP110ASCII
            hmp110_inlet = HMP110ASCII(name='hmp110-inlet', config=config)
            hmp110_inlet.setup_schedules()
            if s3fsc:
                s3fsc.setup_transfer_schedules(
                    local_path=str(hmp110_inlet.staging_path),
                    key_prefix=hmp110_inlet.remote_path,
                    interval=hmp110_inlet.reporting_interval,
                    delay_transfer=3,
                    remove_on_success=False,
                )
            if sftp:
                remote_path = (PurePosixPath(sftp.remote_path) / hmp110_inlet.remote_path).as_posix()
                sftp.setup_transfer_schedules(local_path=hmp110_inlet.staging_path,
                                            remote_path=remote_path,
                                            interval=hmp110_inlet.reporting_interval)

        if config.get('hmp110-ae31', None):
            from nrbdaq.instr.vaisala import HMP110ASCII
            hmp110_ae31 = HMP110ASCII(name='hmp110-ae31', config=config)
            hmp110_ae31.setup_schedules()
            if s3fsc:
                s3fsc.setup_transfer_schedules(
                    local_path=str(hmp110_ae31.staging_path),
                    key_prefix=hmp110_ae31.remote_path,
                    interval=hmp110_ae31.reporting_interval,
                    delay_transfer=3,
                    remove_on_success=False,
                )
            if sftp:
                remote_path = (PurePosixPath(sftp.remote_path) / hmp110_ae31.remote_path).as_posix()
                sftp.setup_transfer_schedules(local_path=hmp110_ae31.staging_path,
                                            remote_path=remote_path,
                                            interval=hmp110_ae31.reporting_interval)

        if config.get('hmp110-lab', None):
            from nrbdaq.instr.vaisala import HMP110ASCII
            hmp110_lab = HMP110ASCII(name='hmp110-lab', config=config)
            hmp110_lab.setup_schedules()
            if s3fsc:
                s3fsc.setup_transfer_schedules(
                    local_path=str(hmp110_lab.staging_path),
                    key_prefix=hmp110_lab.remote_path,
                    interval=hmp110_lab.reporting_interval,
                    delay_transfer=3,
                    remove_on_success=False,
                )
            if sftp:
                remote_path = (PurePosixPath(sftp.remote_path) / hmp110_lab.remote_path).as_posix()
                sftp.setup_transfer_schedules(local_path=hmp110_lab.staging_path,
                                            remote_path=remote_path,
                                            interval=hmp110_lab.reporting_interval)

        # setup Nairobi AVO data download, staging and transfer
        if config.get('AVO', None):
            data_path = Path(config['root']).expanduser() / config['data'] / config['AVO']['data_path']
            staging_path = Path(config['root']).expanduser() / config['staging'] / config['AVO']['staging_path']
            download_interval = config['AVO']['download_interval']
            hours = [f"{download_interval*n:02}:00" for n in range(23) if download_interval*n <= 23]
            for hr in hours:
                schedule.every(1).day.at(hr).do(avo.download_multiple,
                                            urls={'url_nairobi': config['AVO']['urls']['url_nairobi']},
                                            file_path=data_path,
                                            staging=staging_path)
            if s3fsc:
                s3fsc.setup_transfer_schedules(
                    local_path=str(staging_path),
                    key_prefix=config['AVO']['remote_path'],
                    interval=download_interval,
                    delay_transfer=3,
                    remove_on_success=False,
                )
            if sftp:
                remote_path = (PurePosixPath(sftp.remote_path) / config['AVO']['remote_path']).as_posix()
                sftp.setup_transfer_schedules(local_path=staging_path,
                                            remote_path=remote_path,
                                            interval=download_interval)

        # setup Thermo 49i data acquisition and data transfer
        if config.get('thermo49i', None):
            thermo49i = Thermo49i(config=config)
            thermo49i.setup_schedules()
            if s3fsc:
                s3fsc.setup_transfer_schedules(
                    local_path=str(thermo49i.staging_path),
                    key_prefix=thermo49i.remote_path,
                    interval=thermo49i.reporting_interval,
                    delay_transfer=3,
                    remove_on_success=False,
                )
            if sftp:
                remote_path = (PurePosixPath(sftp.remote_path) / thermo49i.remote_path).as_posix()
                sftp.setup_transfer_schedules(local_path=thermo49i.staging_path,
                                            remote_path=remote_path,
                                            interval=thermo49i.reporting_interval)

        # setup Aurora3000
        if config.get('aurora3000', None):
            neph = Aurora3000(config=config)
            neph.setup_schedules()
            logger.info(f"get_instrument_id: {neph.get_instrument_id()}")
            if s3fsc:
                s3fsc.setup_transfer_schedules(
                    local_path=str(neph.staging_path),
                    key_prefix=neph.remote_path,
                    interval=neph.reporting_interval,
                    delay_transfer=3,
                    remove_on_success=False,
                )
            if sftp:    
                remote_path = (PurePosixPath(sftp.remote_path) / neph.remote_path).as_posix()
                sftp.setup_transfer_schedules(local_path=neph.staging_path,
                                            remote_path=remote_path,
                                            interval=neph.reporting_interval)
    except Exception as err:
        logger.error(err)

    # list all jobs
    logger.info(schedule.get_jobs(), extra={'to_logfile': True})

    # align start with a multiple-of-minute timestamp
    seconds_left = seconds_to_next_n_minutes(1)
    while seconds_left > 0:
        print(f"Time remaining (s): {int(seconds_left):>3d}", end="\r")
        dt = 0.5
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
