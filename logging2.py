import os
import pandas as pd
import numpy as np
import json
import logging

logging.basicConfig(
    filename="inconsistent_date.log", filemode="w", format="%(asctime)s %(message)s"
)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

with open(r"C:\Users\arshdeep.singh\Downloads\bank_cmplex_json_NA.json") as file:
    bank_data = json.load(file)


bank_df = pd.json_normalize(bank_data["branches"])
branch_df = bank_df.drop("accounts", axis=1)

account_data = []
for index, row in bank_df.iterrows():
    account_data.extend(row["accounts"])


accounts_df = pd.json_normalize(account_data)

loans_data = []
for index, row in accounts_df.iterrows():
    loans_data.extend(row["loans"])

loans_df = pd.json_normalize(loans_data)

transaction_data = []
for index, row in accounts_df.iterrows():
    transaction_data.extend(row["transactions"])

transactions_df = pd.json_normalize(transaction_data)
print(transactions_df)

# find the customer and credit attributes
col_names = list(accounts_df.columns)
customer_attribute = []
credit_attributes = []
for col in col_names:
    if "." in col and col.split(".")[0] == "customer":
        customer_attribute.append(col.split(".")[1])
    elif "." in col and col.split(".")[0] == "credit":
        credit_attributes.append(col.split(".")[1])

# Not let's rename the column
for col in tuple(accounts_df.columns):
    new_col = col.split(".")[-1]  # split the column name and select the last one

    # now let's rename the old column with new one
    accounts_df.rename({col: new_col}, axis=1, inplace=True)


accounts_df_2 = (
    accounts_df.iloc[:, :9].drop(["loans", "transactions"], axis=1).drop_duplicates()
)

customers_df = accounts_df.iloc[:, 8:13].drop_duplicates().reset_index(drop=True)

credit_df = accounts_df.iloc[:, 13:].drop_duplicates().reset_index(drop=True)


## Error prone code - in ILT Explain them via debugger
def convert_to_datetime(row: pd.Series):
    try:
        actual_date = pd.to_datetime(row["transaction_date"])  
        # Avoid using unit='ms' to prevent incorrect date conversion
        return actual_date
    except ValueError as error:
        logger.error(
            f"Error converting transaction date for ID {row['transaction_id']} at index {row.name}: {error}. Transaction date: {row['transaction_date']}"
        )
        return pd.NaT


# Finally block will execute there is exception or not

## Apply the function to transaction_date via apply function
transactions_df2 = transactions_df.copy()

transactions_df2["transaction_date"] = transactions_df2.apply(
    convert_to_datetime, axis=1
)