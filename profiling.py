from memory_profiler import profile
import json
import pandas as pd
from pandas import json_normalize
 
@profile
def load_data(path):
    with open(path) as f:
        data = json.load(f)
    #Branch Dataframe
    branch_df= json_normalize(data)
    del data
    return branch_df
 
def main():
    df= load_data(r"C:\Users\arshdeep.singh\Downloads\transactions.json")
    print(df.head())

main()