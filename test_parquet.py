import snowflake.connector
from snowflake.snowpark import Session
from snowflake.sqlalchemy import URL
import streamlit as st
import openai
from file_uploader import FileUploader
import pandas as pd

pd.set_option('display.max_columns', None)

# Replace 'your_file.parquet' with the path to your actual Parquet file
df = pd.read_parquet('/workspaces/finance_project/sd254_cards_de.parquet', engine='pyarrow')
# or, if using fastparquet
# df = pd.read_parquet('your_file.parquet', engine='fastparquet')

print(df.info())
print(df.head())


'''
Data columns (total 13 columns):
 #   Column                 Non-Null Count  Dtype 
---  ------                 --------------  ----- 
 0   User                   6146 non-null   int64 
 1   CARD INDEX             6146 non-null   int64 
 2   Card Brand             6146 non-null   object
 3   Card Type              6146 non-null   object
 4   Card Number            6146 non-null   int64 
 5   Expires                6146 non-null   object
 6   CVV                    6146 non-null   int64 
 7   Has Chip               6146 non-null   object
 8   Cards Issued           6146 non-null   int64 
 9   Credit Limit           6146 non-null   object
 10  Acct Open Date         6146 non-null   object
 11  Year PIN last Changed  6146 non-null   int64 
 12  Card on Dark Web       6146 non-null   object


{
    "Card Number": {"astype": "str"),
    "Has Chip": {"map":("YES" :1, "NO"：0)},
    "Expires": {"to_datetime": {"format": "%m/%Y"}},
    "Card Limit": {"astype": "int"},
    "Acct Open Date": {"to_datetime": {"format": "%m/%Y"}},
    "Card on Dark Web": {"map":("YES" :1, "NO"：0)}
}

'''

import pandas as pd
import json

# Your JSON structure
json_data = {
    "Card Number": {"astype": "str"},
    "Has Chip": {"map": {"YES": 1, "NO": 0}},
    "Expires": {"to_datetime": {"format": "%m/%Y"}},
    "Card Limit": {"astype": "int"},
    "Acct Open Date": {"to_datetime": {"format": "%m/%Y"}},
    "Card on Dark Web": {"map": {"YES": 1, "NO": 0}}
}

# Convert the JSON object to a DataFrame
transformation_df = pd.DataFrame([
    {"Column": key, "Transformation": str(value)}
    for key, value in json_data.items()
])
# Print the DataFrame
print(transformation_df)