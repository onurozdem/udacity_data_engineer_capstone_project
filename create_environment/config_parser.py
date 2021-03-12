import configparser
import os

ROOT_PATH = os.path.dirname(__file__)

CONFIG = configparser.ConfigParser()
CONFIG.read(os.path.join(ROOT_PATH, "config.cfg"))