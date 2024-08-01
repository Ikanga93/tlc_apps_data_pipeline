#!/usr/bin/env python

# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import pandas as pd
from sodapy import Socrata

# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
# client = Socrata("data.cityofnewyork.us", None)

MyAppToken = "3n24PhVfrGN0jpYvSlbkFd7M3"
# Example authenticated client (needed for non-public datasets):
client = Socrata("data.cityofnewyork.us",
                 MyAppToken,
                 username="ekuke003@gmail.com", password="D2racine4ac#")

# First 2000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.
results = client.get("dpec-ucu7", limit=6000)

# Convert to pandas DataFrame
results_df = pd.DataFrame.from_records(results)
# print(results_df.info())
# Check duplicates
print(results_df.duplicated().sum())
