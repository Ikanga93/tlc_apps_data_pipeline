# Import necessary library
from sodapy import Socrata
from config import load_config
from datetime import datetime
from retrying import retry
import logging

# Logging configuration
logging.basicConfig(filename="extract.log", level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Config file path
config = load_config("/Users/jbshome/Desktop/tlc_application_etl/configuration/config.json")
logging.info("Configuration file loaded successfully")

# Define the default timestamp
DEFAULT_TIMESTAMP = datetime(1970,1,1)

# Retry decorator to retry the get_last_extraction_time function if there is an error 
@retry(stop_max_delay=10000, wait_fixed=2000, stop_max_attempt_number=7)
# Function to read the last extraction timestamp from a file 
def get_last_extraction_time():
    try:
        # Open the file and read the timestamp
        with open("last_extraction.txt", "r") as file:
            logging.info("Last extraction timestamp file loaded successfully")
            # Skip the first line and read the timestamp
            for line in file:
                if not line.startswith("#"):
                    # Return the timestamp if it is valid 
                    last_extraction = file.read().strip()
                    logging.info(f"Last extraction timestamp: {last_extraction}")
                    # return datetime.fromisoformat(last_extraction)
        # If no valid timestamp found, return default
        return DEFAULT_TIMESTAMP
    except FileNotFoundError:
        logging.error("Last extraction timestamp file not found")
        # Default value if file not found
        return DEFAULT_TIMESTAMP

# Call the function    
get_last_extraction_time()

# Retry decorator to retry the save_current_extraction_time function if there is an error
@retry(stop_max_delay=10000, wait_fixed=2000, stop_max_attempt_number=7)
# Function to save the current extraction timestamp to a file 
def save_current_extraction_time():
    try:
        # Open the file and write the current timestamp
        with open("last_extraction.txt", "w") as file:
            logging.info("Last extraction timestamp file created successfully")
            # Write the current timestamp to the file
            file.write("# Last extraction timestamp\n")
            file.write(datetime.now().isoformat() + "\n")
            logging.info("Current extraction timestamp saved successfully")
            return file
    # Catch errors if the file cannot be created
    except Exception as FileExistsError:
        logging.error("File cannot be created")
    except Exception as e:
        logging.error("An error occurred")
        # Return None if there is an error
        return None
# Call the function   
save_current_extraction_time()

# Retry decorator to retry the extract function if there is an error
@retry(stop_max_delay=10000, wait_fixed=2000, stop_max_attempt_number=7)
# Function to extract data from the API
def extract(*args):
    try:
        last_extraction_time = get_last_extraction_time()
        # Create a Socrata client 
        client = Socrata(*args)
        logging.info("Socrata client created successfully")
        # Query the data from the API using the last extraction timestamp
        query = f'lastupdate >= "{last_extraction_time.isoformat()}"'
        # Extract the data from the API using the query
        data = client.get(config["dpec_code"], where=query, limit=3621)
        logging.info("Data extracted successfully")
        return data
    # Catch errors if there is an error with the API
    except Exception as ExtractError:
        logging.error("Cannot extract data from the API")
    # Catch errors if there is any error with the function
    except Exception as e:
        logging.error("An error occurred")
        # Return None if there is an error
        return None

# Main function
if __name__ == "__main__":
    # Call the extract
    extracted_data = extract(config["tlc_api_url"], config["tlc_app_token"], config["tlc_username"], config["tlc_password"])
    #print(extracted_data)