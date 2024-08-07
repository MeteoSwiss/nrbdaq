import os
import json
import polars as pl
import requests


def get_data(url: str, validated: bool=False) -> dict:
    if validated:
        url = f"{url}/validated_data"

    resp = requests.get(url)
    if resp.ok:
        data = json.loads(resp.text)

    return data


def flatten_data(data: dict, parent_key='', sep='_') -> dict:
    items = []
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_data(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)    
    

def data_to_dfs(data: dict, file_path: str=str()) -> tuple[str, dict]:
    if file_path:
       os.makedirs(file_path, exist_ok=True)

    # Extract name
    station = data['name'].lower().replace(' ', '_')

    result = dict()
    keys = ['instant', 'hourly', 'daily', 'monthly']

    # Extract and flatten data into a list of several polars DataFrames
    values = [pl.DataFrame([flatten_data(entry) for entry in data['historical'][key]]) for key in keys]

    result = dict(zip(keys, values))

    # # Extract and flatten instant data
    # instant_data = [flatten_data(entry) for entry in data['historical']['instant']]
    # instant_df = pl.DataFrame(instant_data)

    # # Extract and flatten hourly data
    # hourly_data = [flatten_data(entry) for entry in data['historical']['hourly']]
    # hourly_df = pl.DataFrame(hourly_data)

    # # Extract and flatten daily data
    # daily_data = [flatten_data(entry) for entry in data['historical']['daily']]
    # daily_df = pl.DataFrame(daily_data)

    # # Extract and flatten monthly data
    # monthly_data = [flatten_data(entry) for entry in data['historical']['monthly']]
    # monthly_df = pl.DataFrame(monthly_data)

    if result:
        for key, value in result.items():
            file = os.path.join(file_path, f"{station}_iqair_{key}.parquet")
            # print(file)
            # print(value.schema)
            value.write_parquet(file)

    return station, result
