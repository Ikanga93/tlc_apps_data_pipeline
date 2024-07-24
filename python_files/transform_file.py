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
    # Create a csv file that record te monthly trend of the number of applications submitted since the beginning of the dataset
    df['month'] = df['application_date'].dt.to_period('M')
    df.groupby('month').size().reset_index(name='count').to_csv("tlc_applications_monthly.csv", index=False)
    

    return df

extracted_data = extract(config["tlc_api_url"], config["tlc_app_token"], config["tlc_username"], config["tlc_password"])
clean_data = transform(extracted_data)
print(clean_data)