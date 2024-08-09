import os
import configparser
import logging

def load_config(config_file: str) -> configparser.ConfigParser:
    """
    Load configuration from a file.

    :param config_file: Path to the configuration file.
    :return: ConfigParser object with the loaded configuration.
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    return config


def setup_logging(file: str) -> logging:
    """Setup the main logging device

    Args:
        file (str): full path to log file

    Returns:
        logging: a logger object
    """
    file = os.path.expanduser(file)
    file_path = os.path.dirname(file)
    os.makedirs(file_path, exist_ok=True)

    main_logger = os.path.basename(file).split('.')[0]
    logger = logging.getLogger(main_logger)
    logger.setLevel(logging.DEBUG)

    # create file handler which logs warning and above messages
    fh = logging.FileHandler(file)
    fh.setLevel(logging.WARNING)

    # create console handler which logs even debugging information
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s, %(levelname)s, %(name)s, %(message)s', datefmt="%Y-%m-%dT%H:%M:%S")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger
