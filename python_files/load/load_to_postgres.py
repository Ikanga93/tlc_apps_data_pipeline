# Import libraries
from transform_file import transform
from extract_file import extract
from config import load_config
from sqlalchemy import create_engine
from retrying import retry
import psycopg2
import logging

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# Function to load data to snowflake

# Config file path
config = load_config("/Users/jbshome/Desktop/tlc_application_etl/config/dev/config.json")
logging.info("Config file successfully loaded")

# Retry decorator to retry the function if there is an error 
@retry(stop_max_delay=10000, wait_fixed=2000, stop_max_attempt_number=7)
# Function to load data to postgres
def load_to_postgres(df):
    # Database parameters 
    db_params = {
        "host": config["host"],
        "database": config["database"],
        "user": config["user"],
        "password": config["password"],
        "port": config["port"]
    }

    try:   
        # Create a connection string
        connection_string = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
        # Create a SQLAlchemy engine
        engine = create_engine(connection_string)

        # Load data to the table
        df.to_sql('tlc_drivers', engine, if_exists='replace', index=False)
        logging.info("Data loaded to Postgres successfully")
        # Commit the transaction (implicitly handled by SQLAlchemy)

    # Catch errors if the connection fails   
    except Exception as ConnectionError:
        logging.error("An error occurred while connecting to the database")
    # Catch errors if the engine cannot be created
    except Exception as EngineError:
        logging.error("An error occurred while creating the engine")
    # Catch errors if the data cannot be loaded to the table
    except Exception as LoadError:
        logging.error("An error occurred while loading the data")
        # Return None if there is an error
        return None
        
    return df

# Main function
if __name__ == "__main__":
    # Extract data from the API 
    extracted_data = extract(config["tlc_api_url"], config["tlc_app_token"], config["tlc_username"], config["tlc_password"])
    # Transform the extracted data
    clean_data = transform(extracted_data)
    # Load the transformed data to a postgres table
    load_to_postgres(clean_data)
