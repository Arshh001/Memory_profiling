from memory_profiler import profile
import json
import pandas as pd
from pandas import json_normalize

@profile
def load_data(path):
    with open(path) as f:
        for line in f:
            data = json.loads(line)
            yield json_normalize(data)

@profile
def main():
    generator = load_data(r"C:\Users\arshdeep.singh\Downloads\transactions.json")
    for df in generator:
        print(df.head())

main()