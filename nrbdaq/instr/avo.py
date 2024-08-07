"""iQAIr Air Visual Outdoor (AVO) data download and parsing. Data are stored in .parquet files."""
import os
import json
import polars as pl
import requests


def get_data(url: str, validated: bool=False) -> dict:
    """
    Download AVO data from the portal. 
    The most recent 60 instant (1-minute), 48 hourly, 30/31 daily, 12 monthly values available.
    It is unclear how data are validated in detail.

    Args:
        url (str): The API call
        validated (bool, optional): If True, only validated data are retrieved. 
        These cover a shorter period. Defaults to False.

    Returns:
        dict: A nested dictionary
    """
    if validated:
        url = f"{url}/validated_data"

    resp = requests.get(url)
    if resp.ok:
        data = json.loads(resp.text)

    return data


def flatten_data(data: dict, parent_key='', sep='_') -> dict:
    """Flatten a nested JSON object

    Args:
        data (dict): an AVO data set
        parent_key (str, optional): ??. Defaults to ''.
        sep (str, optional): _description_. Defaults to '_'.

    Returns:
        dict: flattened dict
    """
    items = []
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_data(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def data_to_dfs(data: dict, file_path: str=str(),
                append: bool=True, remove_duplicates: bool=True) -> tuple[str, dict]:
    """Saves a flattened dictionary as polars DataFrame

    Args:
        data (dict): flattened dict of AVO data
        file_path (str, optional): dictionary path for data files. Defaults to str().
        append (bool, optional): Should existing .parquet files be appended? Defaults to True.
        remove_duplicates (bool, optional): Should duplicates be removed? Defaults to True.

    Returns:
        tuple[str, dict]: station name, dictionary of the various data sets
    """
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
            file = os.path.join(file_path, f"{station}_avo_{key}.parquet")
            if os.path.exists(file) and append:
                value = pl.concat([pl.read_parquet(file), value], how='diagonal')
            if remove_duplicates:
                value = value.unique()
            # print(file)
            # print(value.schema)
            value.write_parquet(file)

    return station, result
