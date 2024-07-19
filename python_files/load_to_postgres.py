# Import libraries
from transform_file import transform
from extract_file import extract
from config import load_config
from sqlalchemy import create_engine
import psycopg2
import logging

# Config file path
config = load_config("/Users/jbshome/Desktop/tlc_application_etl/configuration/config.json")
# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# Function to load data to snowflake
def load_to_postgres(df):
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
        df.to_sql('tlc_drivers', engine, if_exists='append', index=False)
        logging.info("Data loaded to Postgres successfully")
        # Commit the transaction (implicitly handled by SQLAlchemy)
        
    except Exception as error:
        print("Error: %s" % error)
        
    return df

if __name__ == "__main__":
    extracted_data = extract(config["tlc_api_url"], config["tlc_app_token"], config["tlc_username"], config["tlc_password"])
    clean_data = transform(extracted_data)
    load_to_postgres(clean_data)
