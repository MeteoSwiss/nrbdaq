import logging
import os
import time
from pathlib import Path
import yaml


def load_config(config_file: str) -> dict:
    """
    Load configuration from config file.

    :param config_file: Path to the configuration file.
    :return: ConfigParser object with the loaded configuration.
    """
    extension = Path(config_file).expanduser().suffix.lower()
    if extension in ('.yaml', '.yml'):
        with open(config_file, 'r') as fh:
            config = yaml.safe_load(fh)
        if not isinstance(config, dict):
            raise ValueError(f"Config must be a dict at top level, got {type(config)!r} in {config_file}")
        return config

    raise ValueError("Extension of config file not recognized!)")


def setup_logging(file: str, level_console:int=20, level_file:int=40) -> logging.Logger:
    """Setup the main logging device

    Args:
        file (str): full path to log file

    Returns:
        logging.Logger: a logger object
    """

    file_path = os.path.dirname(file)
    os.makedirs(file_path, exist_ok=True)

    main_logger = os.path.basename(file).split('.')[0]
    logger = logging.getLogger(main_logger)
    logger.setLevel(logging.DEBUG)

    # create file handler which logs level_file and above messages
    fh = logging.FileHandler(file)
    fh.setLevel(level_file)

    # File handler for selective INFO logging
    info_fh = logging.FileHandler(file)
    info_fh.setLevel(logging.INFO)
    info_fh.addFilter(lambda record: getattr(record, 'to_logfile', False))

    # create console handler which logs even debugging information
    ch = logging.StreamHandler()
    ch.setLevel(level_console)

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s, %(levelname)s, %(name)s, %(message)s', datefmt="%Y-%m-%dT%H:%M:%S")
    fh.setFormatter(formatter)
    info_fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(info_fh)
    logger.addHandler(ch)

    # mqtt_handler = MQTTHandler()
    # logger.addHandler(mqtt_handler)

    return logger


def seconds_to_next_n_minutes(n: int):
    # Get the current time in seconds since the epoch
    now = time.time()

    # Calculate minutes and seconds of the current time
    minutes = int(now // 60) % 60
    seconds = int(now % 60)

    # Calculate remaining time to the next n-minute mark
    minutes_to_next_n_minutes = n - (minutes % n)
    remaining_seconds = (minutes_to_next_n_minutes * 60) - seconds
    return remaining_seconds
