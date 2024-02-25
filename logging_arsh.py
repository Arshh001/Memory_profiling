import os
import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime
from pandas import json_normalize

logging.basicConfig(filename="inconsistent_date.log", filemode="w", format="%(asctime)s %(message)s")

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

#url = "https://mentorskool-platform-uploads.s3.ap-south-1.amazonaws.com/documents/e9fb5e7c-30c1-4761-9963-07f4ec636777_83d04ac6-cb74-4a96-a06a-e0d5442aa126_bank_cmplex_json.json"
#response = requests.get(url)
#data = response.json()

with open(r"C:\Users\arshdeep.singh\Downloads\bank_cmplex_json_NA.json") as f:
      data = json.load(f)

branch_df= json_normalize(data['branches'])

branch_df= branch_df.drop('accounts', axis=1)
accounts_df= json_normalize(data['branches'], record_path='accounts')
#Customers Dataframe
customers_df = accounts_df.filter(like='customer.')
customers_df.columns= ['customer_id','full_name','customer_email','dob','customer_phone']
#Credit Dataframe
credits_df = accounts_df.filter(like='credit.')
credits_df.columns= ['customer_id','credit_score','annual_income','years_of_credit_history','months_since_last_delinquent','number_of_open_accounts','number_of_credit_problems','current_credit_balance','maximum_open_credit','bankruptcies','tax_liens']
#Loans Dataframe
loans_df= json_normalize(data, record_path=['branches','accounts','loans'])
#Transactions Dataframe
transactions_df= json_normalize(data, record_path=['branches','accounts','transactions'])            
#Accounts Dataframe
accounts_df= accounts_df[['AccountId','CustomerID','AccountType','Balance','last_kyc_updated','branch_id']]



"""

def convert_to_datetime(index: bool=None,transaction_date: str) -> datetime:
      try:
            return pd.to_datetime(transaction_date, unit='ms')
      except ValueError as error:
            logger.error(f"(The data present at the index contains an inconsistent date - {transaction_date}")
            return pd.NaT
    
"""
def convert_to_datetime(row: pd.Series):
    try:
        actual_date = pd.to_datetime(row["transaction_date"])  
        # Avoid using unit='ms' to prevent incorrect date conversion
        return actual_date
    except ValueError as error:
        logger.error(
            f"The data present at the index {row.name} contains an inconsistent date - {row['transaction_date']}"
        )
        return pd.NaT


# Finally block will execute there is exception or not

## Apply the function to transaction_date via apply function
transactions_df2 = transactions_df.copy()

transactions_df2["transaction_date"] = transactions_df2.apply(
    convert_to_datetime, axis=1
)
      

