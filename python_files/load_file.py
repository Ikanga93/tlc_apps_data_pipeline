# Import libraries
import csv
from transform_file import transform
from extract_file import extract
from config import load_config

# Config file path
config = load_config("/Users/jbshome/Desktop/tlc_application_etl/configuration/config.json")

# CSV file_path
csv_file = "/Users/jbshome/Desktop/tlc_application_etl/csv_files/csv_file.csv"
# Fucntion to load data to a csv file
def load(df):
    df = df.to_csv(csv_file, index=False)
    return df

if __name__ == "__main__":
    extracted_data = extract(config["tlc_api_url"], config["tlc_app_token"], config["tlc_username"], config["tlc_password"])
    clean_data = transform(extracted_data)
    load(clean_data)