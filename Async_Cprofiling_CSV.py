import pandas as pd
import os
import csv
import cProfile
import pstats
import asyncio

async def fetch_dataframe(filename):
    data = []
    with open(filename, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            data.append(row)
    df = pd.DataFrame(data)
    await asyncio.sleep(1)
    return df

async def process_files(path):
    category_dataframes = {}
    tasks = []
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith('.csv'):
                filename = os.path.join(root, file)
                tasks.append(fetch_dataframe(filename))
    results = await asyncio.gather(*tasks)
    for idx, (root, _, files) in enumerate(os.walk(path)):
        for file in files:
            if file.endswith('.csv'):
                filename = os.path.join(root, file)
                category_dataframes[file.split('.')[0]] = results[idx]
                break  # Assuming one dataframe per folder
    return category_dataframes

if __name__ == '__main__':
    path = r"C:\Users\arshdeep.singh\Downloads\57ccb670-94e4-424b-a13d-ede4d22849ae_83d04ac6-cb74-4a96-a06a-e0d5442aa126_banking_data (2)\banking_data"

    profiler = cProfile.Profile()
    profiler.enable()

    asyncio.run(process_files(path))

    profiler.disable()

    with open('async_csv_profiling_results.txt', 'w') as f:
        stats = pstats.Stats(profiler, stream=f)
        stats.sort_stats(pstats.SortKey.TIME)
        stats.print_stats()
