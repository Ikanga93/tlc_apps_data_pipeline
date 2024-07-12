# Import necessary library
from sodapy import Socrata
from config import load_config

# Config file path
config = load_config("/Users/jbshome/Desktop/tlc_application_etl/configuration/config.json")
# Function to extract data
def extract(*args):
    client = Socrata(*args)
    
    data = client.get(config["dpec_code"], limit=3621)
    return data

# extract(config["tlc_api_url"], config["tlc_app_token"], config["tlc_username"], config["tlc_password"])
extracted_data = extract(config["tlc_api_url"], config["tlc_app_token"], config["tlc_username"], config["tlc_password"])
# print(extracted_data)