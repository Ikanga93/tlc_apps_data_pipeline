# Eliminate redundancy by loading the configuration once in a centralized location
import json
import logging

# Config load function
def load_config(file_path):
    try:
        with open(file_path, "r") as file:
            config = json.load(file)
            logging.info("Configuration file loaded successfully") 
            return config
        
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {file_path}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON: {e}")
        raise

# Load the configuration
config = load_config("/Users/jbshome/Desktop/etl_folder/tlc_application_etl/config/config.json")

