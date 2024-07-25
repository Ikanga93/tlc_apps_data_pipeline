# Eliminate redundancy by loading the configuration once in a centralized location
import json

# Config file path
config_file_path = "/Users/jbshome/Desktop/tlc_application_etl/config/dev/config.json"

# Config load function
def load_config(config_file_path):
    with open(config_file_path, "r") as config_file:
        config = json.load(config_file)
    return config
