import configparser
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Literal, overload

import paho.mqtt.client as mqtt
import yaml


class MQTTHandler(logging.Handler):
    def __init__(self, broker: str = "localhost", port: int = 1883, topic: str = "logs"):
        self.client = mqtt.Client()
        self.client.connect(broker, port, 60)
        self.topic = topic

    def emit(self, record):
        log_entry = self.format(record)
        self.client.publish(self.topic, log_entry)


@overload
def load_config(config_file: str, *, as_dict: Literal[True]) -> dict[str, Any]:
    ...


@overload
def load_config(
    config_file: str, *, as_dict: Literal[False] = False
) -> dict[str, Any] | configparser.ConfigParser:
    ...


def load_config(
    config_file: str,
    *,
    as_dict: bool = False,
) -> dict[str, Any] | configparser.ConfigParser:
    """Load a configuration file.

    Supports YAML (".yml"/".yaml") and INI (".ini").

    - For YAML, this returns a dictionary (top-level mapping required).
    - For INI, this returns ``configparser.ConfigParser`` by default, or a
      dictionary view when ``as_dict=True``.

    The overloads let Pylance infer the correct type when you call
    ``load_config(..., as_dict=True)``.

    Args:
        config_file: Path to the configuration file.
        as_dict: For INI files, return a dict view instead of ``ConfigParser``.

    Returns:
        The loaded configuration as a dictionary or ``ConfigParser``.

    Raises:
        ValueError: If the file extension is unsupported.
        TypeError: If a YAML file does not contain a top-level mapping.
    """
    ext = Path(config_file).suffix.lower().lstrip(".")

    if ext in {"yaml", "yml"}:
        with open(config_file, "r", encoding="utf-8") as fh:
            loaded = yaml.safe_load(fh) or {}
        if not isinstance(loaded, dict):
            raise TypeError("YAML config must be a mapping at the top level (e.g. key: value).")
        return loaded

    if ext == "ini":
        parser = configparser.ConfigParser()
        parser.read(config_file)
        if not as_dict:
            return parser

        # Best-effort conversion to a plain dict.
        data: dict[str, Any] = {}
        if parser.defaults():
            data["DEFAULT"] = dict(parser.defaults())
        for section in parser.sections():
            data[section] = dict(parser[section])
        return data

    raise ValueError(f"Unsupported config file extension: {ext!r}")


def setup_logging(file: str, level_console: int = 20, level_file: int = 40) -> logging.Logger:
    """Setup the main logging device.

    - Console: level_console and above.
    - File: level_file and above (typically ERROR+), plus INFO lines explicitly
      marked with `extra={"to_logfile": True}`.

    Args:
        file: Full path to log file.
        level_console: Console log level.
        level_file: File log level (default ERROR).

    Returns:
        A configured logger.
    """
    os.makedirs(os.path.dirname(file), exist_ok=True)

    main_logger = os.path.basename(file).split(".")[0]
    logger = logging.getLogger(main_logger)
    logger.setLevel(logging.DEBUG)

    # Prevent double-logging via root handlers
    logger.propagate = False

    # Avoid duplicate handlers if setup_logging is called again
    for h in list(logger.handlers):
        logger.removeHandler(h)
        try:
            h.close()
        except Exception:
            pass

    formatter = logging.Formatter(
        "%(asctime)s, %(levelname)s, %(name)s, %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # File handler for errors (and above)
    fh = logging.FileHandler(file)
    fh.setLevel(level_file)
    fh.setFormatter(formatter)

    # File handler for selected INFO messages only
    info_fh = logging.FileHandler(file)
    info_fh.setLevel(logging.INFO)
    info_fh.setFormatter(formatter)
    info_fh.addFilter(lambda record: getattr(record, "to_logfile", False))

    # Console handler
    ch = logging.StreamHandler(stream=sys.stdout)  # or omit stream=... to use stderr
    ch.setLevel(level_console)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(info_fh)
    logger.addHandler(ch)

    return logger


def seconds_to_next_n_minutes(n: int) -> float:
    now = time.time()
    minutes = int(now // 60) % 60
    seconds = int(now % 60)
    minutes_to_next_n_minutes = n - (minutes % n)
    return (minutes_to_next_n_minutes * 60) - seconds


# import configparser
# import logging
# import os
# import time

# import paho.mqtt.client as mqtt
# import yaml


# class MQTTHandler(logging.Handler):
#     def __init__(self, broker: str='localhost', port: int=1883, topic: str='logs'):
#         self.client = mqtt.Client()
#         self.client.connect(broker, port, 60)
#         self.topic = topic

#     def emit(self, record):
#         log_entry = self.format(record)
#         self.client.publish(self.topic, log_entry)


# def load_config(config_file: str) -> dict | configparser.ConfigParser:
#     """
#     Load configuration from config file.

#     :param config_file: Path to the configuration file.
#     :return: ConfigParser object with the loaded configuration.
#     """
#     extension = os.path.basename(config_file).split(".")[1].lower()
#     if extension == "ini":
#         config = configparser.ConfigParser()
#         config.read(config_file)
#         return config
#     elif extension == 'yaml' or extension == 'yml':
#         with open(config_file, 'r') as fh:
#             config = yaml.safe_load(fh)
#         return config
#     else:
#         print("Extension of config file not recognized!)")
#         return configparser.ConfigParser()


# def setup_logging(file: str, level_console:int=20, level_file:int=40) -> logging.Logger:
#     """Setup the main logging device

#     Args:
#         file (str): full path to log file

#     Returns:
#         logging.Logger: a logger object
#     """

#     file_path = os.path.dirname(file)
#     os.makedirs(file_path, exist_ok=True)

#     main_logger = os.path.basename(file).split('.')[0]
#     logger = logging.getLogger(main_logger)
#     logger.setLevel(logging.DEBUG)

#     # create file handler which logs level_file and above messages
#     fh = logging.FileHandler(file)
#     fh.setLevel(level_file)

#     # File handler for selective INFO logging
#     info_fh = logging.FileHandler(file)
#     info_fh.setLevel(logging.INFO)
#     info_fh.addFilter(lambda record: getattr(record, 'to_logfile', False))

#     # create console handler which logs even debugging information
#     ch = logging.StreamHandler()
#     ch.setLevel(level_console)

#     # create formatter and add it to the handlers
#     formatter = logging.Formatter('%(asctime)s, %(levelname)s, %(name)s, %(message)s', datefmt="%Y-%m-%dT%H:%M:%S")
#     fh.setFormatter(formatter)
#     info_fh.setFormatter(formatter)
#     ch.setFormatter(formatter)

#     # add the handlers to the logger
#     logger.addHandler(fh)
#     logger.addHandler(info_fh)
#     logger.addHandler(ch)

#     # mqtt_handler = MQTTHandler()
#     # logger.addHandler(mqtt_handler)

#     return logger


# def seconds_to_next_n_minutes(n: int):
#     # Get the current time in seconds since the epoch
#     now = time.time()

#     # Calculate minutes and seconds of the current time
#     minutes = int(now // 60) % 60
#     seconds = int(now % 60)

#     # Calculate remaining time to the next n-minute mark
#     minutes_to_next_n_minutes = n - (minutes % n)
#     remaining_seconds = (minutes_to_next_n_minutes * 60) - seconds
#     return remaining_seconds
