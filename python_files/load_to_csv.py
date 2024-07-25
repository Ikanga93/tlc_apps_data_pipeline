# Import libraries
import csv
from transform_file import transform
from extract_file import extract
from config import load_config
from retrying import retry
import logging

# Logging configuration
logging.basicConfig(filename="csv_file.log", level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# Config file path
config = load_config("/Users/jbshome/Desktop/tlc_application_etl/configuration/config.json")
logging.info("Config file successfully loaded")


# CSV file_path
csv_file = "/Users/jbshome/Desktop/tlc_application_etl/csv_files/csv_file.csv"

# Retry decorator to retry the load function if there is an error
@retry(stop_max_delay=10000, wait_fixed=2000, stop_max_attempt_number=7)
# Fucntion to load data to a csv file
def load(df):
    try:
        # Save the data to a csv file 
        df = df.to_csv(csv_file, index=False)
        logging.info("Data saved to csv file successfully")
        return df
    # Catch errors if the data cannot be saved to a csv file
    except Exception as LoadError:
        logging.error("An error occurred while saving the data to a csv file")
        # Return None if there is an error
        return None

# Main function
if __name__ == "__main__":
    # Extract data from the API 
    extracted_data = extract(config["tlc_api_url"], config["tlc_app_token"], config["tlc_username"], config["tlc_password"])
    # Transform the extracted data 
    clean_data = transform(extracted_data)
    # Load the transformed data to a csv file
    load(clean_data)

# Function to load data to postgres
# def load_to_postgres(df):