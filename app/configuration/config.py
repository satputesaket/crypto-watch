import configparser
from pathlib import Path

CONFIG_FILE = Path(__file__).parent / "configurations.ini"

config = configparser.ConfigParser()
config.read(CONFIG_FILE)