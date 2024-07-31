# Import necessary library
from sodapy import Socrata
from config import load_config
from datetime import datetime
from retrying import retry
import logging
from data_validation import DataValidator

# Config file path
config_file_path = "/Users/jbshome/Desktop/etl_folder/tlc_application_etl/config/config.json"
config = load_config(config_file_path)
logging.info("Config file not found")

# Logging configuration
logging.basicConfig(**config["logging"])

# Initialize the DataValidator instance
validator = DataValidator(config_file=config_file_path)

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
# get_last_extraction_time()

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
# save_current_extraction_time()

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
        data = client.get(config["api_params"]["dpec_code"], where=query, limit=3621)
        logging.info("Data extracted successfully")

        # Validate each record against the schema. If a record does not conform to the schema, a ValidationError is raised.
        for record in data:
            # Ensure `app_no` is a number
            if 'app_no' in record:
                app_no_value = record['app_no']
                if isinstance(app_no_value, str):
                    try:
                        record['app_no'] = float(app_no_value)  # Convert to float for broader number support
                        logging.debug(f"Converted app_no to number: {record['app_no']}")
                    except ValueError:
                        logging.error(f"Cannot convert app_no value '{app_no_value}' to number")
                else:
                    logging.error(f"app_no value '{app_no_value}' is not a string")
            else:
                continue

        # Perform data validation
        validator.validate_schema(data)
        validator.validate_format_and_type(data, column_formats={"app_no": "number", "type": str, "app_date": str, "status": str, "other_requirements": str, "lastupdate": str})
        validator.check_null_and_missing_values(data, required_columns=["app_no", "type", "app_date", "status", "other_requirements", "lastupdate"])
        validator.check_range(data, column_ranges={"app_no": (0, 1000000000)})
        validator.detect_duplicates(data, unique_columns=["app_no"])
        validator.check_consistency(data, consistency_rules=[{"fields": ("app_no", "type"), "condition": lambda x, y: x in y}])
        validator.cross_field_validation(data, cross_field_rules=[{"fields": ["app_no", "status"], "condition": lambda x, y: x != y}])
        validator.validate_pattern(data, patterns={"app_date": r"\d{4}-\d{2}-\d{2}"})
        validator.logger.info("Data validation completed")

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
    extracted_data = extract(config["api_params"]["tlc_api_url"], config["api_params"]["tlc_app_token"], config["api_params"]["tlc_username"], config["api_params"]["tlc_password"])
    print(extracted_data.head())