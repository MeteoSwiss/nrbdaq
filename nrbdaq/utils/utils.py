import os
import configparser
import logging
import yaml
import paho.mqtt.client as mqtt

# MQTT setup
broker = "localhost"  # or use the broker's IP address
port = 1883
topic = "logs"

client = mqtt.Client()
client.connect(broker, port, 60)

class MQTTHandler(logging.Handler):
    def emit(self, record):
        log_entry = self.format(record)
        client.publish(topic, log_entry)


def load_config(config_file: str) -> configparser.ConfigParser:
    """
    Load configuration from config file.

    :param config_file: Path to the configuration file.
    :return: ConfigParser object with the loaded configuration.
    """
    extension = os.path.basename(config_file).split(".")[1].lower()
    if extension == "ini": 
        config = configparser.ConfigParser()
        config.read(config_file)
    elif extension == 'yaml' or extension == 'yml':
        with open(config_file, 'r') as fh:
            config = yaml.safe_load(fh)
    else:
        print("Extension of config file not recognized!)")
    return config


def setup_logging(file: str) -> logging:
    """Setup the main logging device

    Args:
        file (str): full path to log file

    Returns:
        logging: a logger object
    """
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

    mqtt_handler = MQTTHandler()
    logger.addHandler(mqtt_handler)

    return logger
