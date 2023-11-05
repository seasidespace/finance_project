import pandas as pd
import numpy as np


# Assuming num_samples is defined and is a large number
num_samples = 100

# Define a start date that, when added to num_samples, will not go out of bounds
start_date = '2021-01-01'

# Calculate the end date to ensure it's within pandas Timestamp bounds
end_date = pd.Timestamp(start_date) + pd.DateOffset(months=num_samples - 1)

# If the end date is out of bounds, you need to adjust your num_samples or start_date
if end_date.year > 2262:
    raise ValueError("The end date is out of bounds. Adjust the num_samples or start_date.")

# Now create the date range safely
date_range = pd.date_range(start=start_date, periods=num_samples, freq='M').strftime('%m/%Y')

# Define the DataFrame from the parquet data dictionary
parquet_data_df = pd.DataFrame({
    "User": np.random.randint(1, 10000, num_samples),
    "CARD INDEX": np.random.randint(1, 10000, num_samples),
    "Card Brand": np.random.choice(['Visa', 'MasterCard', 'Amex'], num_samples),
    "Card Type": np.random.choice(['Debit', 'Credit'], num_samples),
    "Card Number": np.random.randint(1e15, 1e16, num_samples),  # 16 digit card number
    "Expires": pd.date_range(start='2021-01-01', periods=num_samples, freq='M').strftime('%m/%Y'),
    "CVV": np.random.randint(100, 1000, num_samples),
    "Has Chip": np.random.choice(['YES', 'NO'], num_samples),
    "Cards Issued": np.random.randint(1, 5, num_samples),
    "Credit Limit": np.random.choice(['5000', '10000', '20000'], num_samples),
    "Acct Open Date": pd.date_range(start='2010-01-01', periods=num_samples, freq='M').strftime('%m/%Y'),
    "Year PIN last Changed": np.random.randint(2000, 2023, num_samples),
    "Card on Dark Web": np.random.choice(['YES', 'NO'], num_samples),
})

# Define the transformation rules
json_rules = [
    {"Column": "Card Number", "Transformation": {'astype': 'str'}},
    {"Column": "Has Chip", "Transformation": {'map': {'YES': 1, 'NO': 0}}},
    {"Column": "Expires", "Transformation": {'to_datetime': {'format': '%m/%Y'}}},
    {"Column": "Card Limit", "Transformation": {'astype': 'int'}},
    {"Column": "Acct Open Date", "Transformation": {'to_datetime': {'format': '%m/%Y'}}},
    {"Column": "Card on Dark Web", "Transformation": {'map': {'YES': 1, 'NO': 0}}}
]

# Apply the transformations
for rule in json_rules:
    column = rule["Column"]
    transformation = rule["Transformation"]
    if column in parquet_data_df.columns:
        for operation, params in transformation.items():
            if operation == 'astype':
                parquet_data_df[column] = parquet_data_df[column].astype(params)
            elif operation == 'map':
                parquet_data_df[column] = parquet_data_df[column].map(params)
            elif operation == 'to_datetime':
                parquet_data_df[column] = pd.to_datetime(parquet_data_df[column], format=params['format'])
            else:
                print(f"Unsupported operation: {operation}")

# Check the DataFrame after transformations
print(parquet_data_df.head())
