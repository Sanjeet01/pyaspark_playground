"""
This file contains the functions to read configurations from file and return their values

"""

import configparser
import os

config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
config.read(config_path)


def read_config(section, config_data):
    """
    This function provides the config_utils value for the given input
    :param section:
    :param config_data:
    :return:
    """
    return config.get(section, config_data)
