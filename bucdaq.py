import time
from pathlib import Path, PurePosixPath
from typing import Any

import schedule

from nrbdaq.instr.thermo import Thermo49i
from nrbdaq.utils.s3fsc import S3FSC
from nrbdaq.utils.sftp import SFTPClient
from nrbdaq.utils.utils import (load_config, seconds_to_next_n_minutes,
                                setup_logging)


def main():
    # load configuration (as_dict=True => Pylance infers dict[str, Any])
    config_file = "bucdaq.yml"
    config: dict[str, Any] = load_config(config_file=config_file, as_dict=True)

    # setup logging
    root = Path(str(config["root"])).expanduser()
    logfile = root / str(config["logging"]["file"])
    logger = setup_logging(file=str(logfile))
    logger.info("== Start BUCDAQ =============", extra={"to_logfile": True})

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
    elif config.get("sftp"):
        # Optional fallback if S3 is not configured
        sftp = SFTPClient(config=config)
    else:
        raise RuntimeError("Neither S3 nor sftp is configured in %s!", config_file)

    # setup Thermo 49i data acquisition and data transfer
    if config.get('49i', None):
        thermo49i = Thermo49i(config=config)
        thermo49i.setup_schedules()
        if sftp:
            # remote_path = (PurePosixPath(sftp.remote_path) / thermo49i.remote_path).as_posix()
            # sftp.setup_transfer_schedules(local_path=thermo49i.staging_path,
            #                             remote_path=remote_path,
            #                             interval=thermo49i.reporting_interval)

            # remote paths are POSIX-like; keep them as strings for the sftp layer
            remote_path = f"{sftp.remote_path.rstrip('/')}/{thermo49i.remote_path.lstrip('/')}"
            sftp.setup_transfer_schedules(
                local_path=thermo49i.staging_path,
                remote_path=remote_path,
                interval=thermo49i.reporting_interval,
                remove_on_success=False
            )
        if s3fsc:
            s3fsc.setup_transfer_schedules(
                local_path=str(thermo49i.staging_path),
                key_prefix=thermo49i.remote_path,
                interval=thermo49i.reporting_interval,
                delay_transfer=3,
                remove_on_success=True,
            )

    # list all jobs
    logger.info(schedule.get_jobs(), extra={"to_logfile": True})

    # align start with a multiple-of-minute timestamp
    seconds_left = seconds_to_next_n_minutes(1)
    while seconds_left > 0:
        # logging.Logger.info() does not support `end=...` (that's for print()).
        print(f"Time remaining: {seconds_left:>0.1f} s", end="\r", flush=True)
        dt = 1
        time.sleep(dt)
        seconds_left -= dt

    logger.info("Beginning data acquisition and file transfer ...")

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping data acquisition ...")


if __name__ == "__main__":
    main()