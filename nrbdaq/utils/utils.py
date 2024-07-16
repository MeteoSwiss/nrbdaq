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

def setup_logger(name, file: str='app.log', level_file: str='DEBUG', level_console: str='WARNING') -> logging.Logger:
    """
    Setup logger for the application.
    """
    logger = logging.getLogger(name)
    logger.setLevel('DEBUG')

    # create file handler
    file = os.path.expanduser(file)
    os.makedirs(os.path.dirname(file), exist_ok=True)
    fh = logging.FileHandler(file)
    fh.setLevel(level_file)
    
    # create console handler
    ch = logging.StreamHandler()
    ch.setLevel(level_console)
    
    # create formatter and add it to the handlers
    formatter = logging.Formatter('{dtm: %(asctime)s, source: %(name)s, level: %(levelname)s, msg: %(message)s}')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger
