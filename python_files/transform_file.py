# Import libraries
import pandas as pd
from extract_file import extract
from config import load_config

# Config file path
config = load_config("/Users/jbshome/Desktop/tlc_application_etl/configuration/config.json")

# Function to transform data
def transform(data):
    # Convert to pandas DataFrame
    df = pd.DataFrame.from_records(data)
    return df

extracted_data = extract(config["tlc_api_url"], config["tlc_app_token"], config["tlc_username"], config["tlc_password"])
clean_data = transform(extracted_data)
print(clean_data)