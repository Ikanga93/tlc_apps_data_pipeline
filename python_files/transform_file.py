# Import libraries
import pandas as pd
from extract_file import extract
from config import load_config
from retrying import retry
import logging

# Logging configuration
logging.basicConfig(filename="transform.log", level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Config file path
config = load_config("/Users/jbshome/Desktop/tlc_application_etl/config/dev/config.json")
logging.info("Config file successfully loaded")

# Retry decorator to retry the transform function if there is an error
@retry(stop_max_delay=10000, wait_fixed=2000, stop_max_attempt_number=7)
# Function to transform data
def transform(data):
    try:
        # Convert to pandas DataFrame
        df = pd.DataFrame.from_records(data)
        logging.info("Dataframe successfully created")
        # Rename columns
        df = df.rename(columns={'app_date':'application_date', 'type':'license_type', 'lastupdate':'last_update'})
        # Create a new column that count the number of days from the application date to the last update
        df['days_since_last_update'] = (pd.to_datetime(df['last_update']) - pd.to_datetime(df['application_date'])).dt.days
        # Remove the rows whhere the days since last update is more than 180 days
        df = df[df['days_since_last_update'] <= 180]
        # Create a csv file for the rows where the days since last update is equal to 0 days
        df[df['days_since_last_update'] == 0].to_csv("tlc_applications_submitted.csv", index=False)
        # Create a txt file that record the number of rows and the current date where the days since last update is equal to 0 days
        with open("tlc_applications_submitted.txt", "w") as file:
            file.write(f"Number of rows: {df[df['days_since_last_update'] == 0].shape[0]}\n")
            file.write(f"Date: {pd.to_datetime('now').strftime('%Y-%m-%d')}\n")
        # Convert the application date to timestamp
        df['application_date'] = pd.to_datetime(df['application_date'])
        # Convert the last update to timestamp
        df['last_update'] = pd.to_datetime(df['last_update'])

        return df
    # Catches errors if there is not data
    except ValueError as e:
        logging.error("No data found")
    # Catches errors if there is an error with the data
    except Exception as e:
        logging.error("An error occurred")

# Main function
if __name__ == "__main__":
    # Extract data from the API
    extracted_data = extract(config["tlc_api_url"], config["tlc_app_token"], config["tlc_username"], config["tlc_password"])
    # Transform the extracted data  
    clean_data = transform(extracted_data)
    print(clean_data)