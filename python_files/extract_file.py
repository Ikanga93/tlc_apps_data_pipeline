# Import necessary library
from sodapy import Socrata
from config import load_config
from datetime import datetime
import logging

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Config file path
config = load_config("/Users/jbshome/Desktop/tlc_application_etl/configuration/config.json")

# Define the default timestamp
DEFAULT_TIMESTAMP = datetime(1970,1,1)

# Function to read the last extraction timestamp from a file
def get_last_extraction_time():
    try:
        with open("last_extraction.txt", "r") as file:
            for line in file:
                if not line.startswith("#"):
                    last_extraction = file.read().strip()
                    # return datetime.fromisoformat(last_extraction)
        # If no valid timestamp found, return default
        return DEFAULT_TIMESTAMP
    except FileNotFoundError:
        # Default value if file not found
        return DEFAULT_TIMESTAMP
    
get_last_extraction_time()

# Function to save the current extraction timestamp to a file
def save_current_extraction_time():
    with open("last_extraction.txt", "w") as file:
        file.write("# Last extraction timestamp\n")
        file.write(datetime.now().isoformat() + "\n")
        return file
    
save_current_extraction_time()

# Function to extract data
def extract(*args):
    last_extraction_time = get_last_extraction_time()
    client = Socrata(*args)
    query = f'lastupdate >= "{last_extraction_time.isoformat()}"'
    data = client.get(config["dpec_code"], where=query, limit=3621)
    return data

if __name__ == "__main__":
    extracted_data = extract(config["tlc_api_url"], config["tlc_app_token"], config["tlc_username"], config["tlc_password"])
    #print(extracted_data)