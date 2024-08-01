import logging
import json
import logging
import re
import logging.config
import pandas as pd
# from extract_file import extract 
from config import load_config
from jsonschema import validate, ValidationError

# Class to validate the data records
class DataValidator:
    # Initialize the class with the config file
    def __init__(self, config_file):
        self.logger = logging.getLogger(__name__)
        self.config = self.load_config(config_file)
        self.setup_logging()
        self.schema = self.config.get("schema", {}) # Load the schema from the config file

    # Method to load the configuration file
    def load_config(self, file_path):
        try:
            with open(file_path, "r") as file:
                config = json.load(file)
                self.logger.info("Configuration file loaded successfully")
                return config
        except FileNotFoundError:
            self.logger.error(f"Configuration file not found: {file_path}")
            raise 
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding JSON: {e}")
            raise

    # Method to setup the logging configuration
    def setup_logging(self):
        log_config = self.config.get("logging", {})
        if log_config:
            try:
                logging.basicConfig(
                    filename=log_config.get("log_file", "logs/tlc.log"),
                    level=getattr(logging, log_config.get("log_level", "INFO")),
                    format=log_config.get("log_format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
                    datefmt=log_config.get("log_datefmt", "%Y-%m-%d %H:%M:%S")
                )
                self.logger.info("Logging configured successfully")
            except Exception as e:
                self.logger.error(f"Unexpected error loading logging configuration: {e}")
        
    # Method to validate the schema of the data records
    def validate_schema(self, data):
        if not self.schema:
            self.logger.warning("No schema defined for data validation")
            return 
        
        try:
            for record in data:
                validate(instance=record, schema=self.schema)
                self.logger.info(f"Schema validation successful for record: {record}")
        except ValidationError as e:
            self.logger.error(f"Schema validation failed for record: {e.message}")

    # Method to validate format and type of the data records
    def validate_format_and_type(self, data, format_type_rules):
        for column, expected_type in format_type_rules.items():
            for record in data:
                try:
                    value = record.get(column)
                    if not isinstance(value, expected_type):
                        self.logger.error(f"Data type validation failed for column '{column}': " 
                                          f"Expected {expected_type}, got {type(value)}. Record: {record}")
                except Exception as e:
                    self.logger.error(f"Error in format and type check for columns '{column}' "
                                      f"with value '{value}': {e}"
                                      )

    # Method to check for null and missing values in the data records
    def check_null_and_missing_values(self, data, required_columns):
        for record in data:
            for column in required_columns:
                try:
                    if record.get(column) is None:
                        self.logger.warning(f"Null value found in column '{column}'")
                except Exception as e:
                    self.logger.error(f"Error in null and missing values check: {e}")

    # Method to check the range of values in the data records
    def check_range(self, data, column_ranges):
        for column, (min_value, max_value) in column_ranges.items():
            for record in data:
                try:
                    value = record.get(column)
                    if not (min_value <= value <= max_value):
                        self.logger.error(f"Range check failed for column '{column}': Value {value} is out of range")
                except Exception as e:
                    self.logger.error(f"Error in range check: {e}")
    
    # Method to check for duplicates in the data records
    def detect_duplicates(self, data, unique_columns):
        try:
            seen = set()
            duplicates = set()
            for record in data:
                values = tuple(record.get(col) for col in unique_columns)
                if values in seen:
                    duplicates.add(values)
                else:
                    seen.add(values)
            if duplicates:
                self.logger.warning(f"Duplicate records found: {duplicates}")
        except Exception as e:
            self.logger.error(f"Error in duplicate detection: {e}")

    # Method to check the uniqueness of the data records
    def cross_field_validation(self, data, cross_field_rules):
        try:
            for rule in cross_field_rules:
                field1, field2, condition = rule['fields'], rule['condition']
                for record in data:
                    if not condition(record.get(field1), record.get(field2)):
                        self.logger.error(f"Cross-field validation failed between fields '{field1}' and '{field2}'")
        except Exception as e:
            self.logger.error(f"Error in cross-field validation: {e}")

    # Method to validate the pattern of the data records
    def validate_pattern(self, data, patterns):
        try:
            for column, pattern in patterns.items():
                regex = re.compile(pattern)
                for record in data:
                    value = record.get(column)
                    if not regex.match(value):
                        self.logger.error(f"Pattern check failed for column '{column}': Value '{value}' does not match pattern '{pattern}'")
        except Exception as e:
            self.logger.error(f"Error in pattern validation: {e}")
