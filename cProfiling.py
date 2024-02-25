import pandas as pd
from pandas import json_normalize
import json
import os
import cProfile
import pstats
import time
 
def fetch_dataframe(filename):
    with open(filename) as f:
        data = json.load(f)
    df = json_normalize(data)
 
    time.sleep(1)
    return df
 
def main():
    path = r"C:\Users\arshdeep.singh\Downloads\aba28d9f-6423-4e6d-976e-11ffe20f5682_83d04ac6-cb74-4a96-a06a-e0d5442aa126_bank_json_data\bank_json_data"
    category_dataframes = {}
    for file in os.listdir(path):
        category_dataframes[file.split('.')[0]] = fetch_dataframe(os.path.join(path, file))
 
if __name__ == '__main__':
    profiler = cProfile.Profile()
    profiler.enable()
    main()
    profiler.disable()
    
    with open('profiling_results.txt', 'w') as f:
        stats = pstats.Stats(profiler, stream=f)
        stats.sort_stats(pstats.SortKey.TIME)
        stats.print_stats()
