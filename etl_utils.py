import pandas as pd
import numpy as np
from datetime import datetime

# ETLUtils class
class ETLUtils:
    # Constructor method
    def __init__(self, df):
        self.df = df

    # Method to validate the data
    def validate_data(self, df):
        # Check if the data is empty
        if df.empty:
            raise ValueError("Data is empty")
        # Check if the data has the correct columns
        if not all(col in df.columns for col in ['driver_id', 'first_name', 'last_name', 'email', 'phone_number', 'date_of_birth', 'city', 'state', 'zip_code', 'car_make', 'car_model', 'car_year', 'car_color', 'car_license_plate']):
            raise ValueError("Data has incorrect columns")
        # Check if the data types are correct
        if not all(isinstance(df[col], (str, np.str_)) for col in ['driver_id', 'first_name', 'last_name', 'email', 'phone_number', 'city', 'state', 'zip_code', 'car_make', 'car_model', 'car_color', 'car_license_plate']):
            raise ValueError("Data has incorrect data types")
        # Schema validation
        if not all(isinstance(df['date_of_birth'], datetime)):
            raise ValueError("Data has incorrect data types")
        # Check if the data has any missing values
        if df.isnull().values.any():
            raise ValueError("Data has missing values")
        # Check if the data has any duplicates
        if df.duplicated().any():
            raise ValueError("Data has duplicates")
        return df
    
    # Method to parse the date of last_update and application_date
    def parse_dates(self, df):
        df['last_update'] = pd.to_datetime(df['last_update'])
        df['application_date'] = pd.to_datetime(df['application_date'])
    
        return df
    
    # Method to  the data

    