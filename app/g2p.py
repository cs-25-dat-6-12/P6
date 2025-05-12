from io import StringIO
import os
import math
import sys
from concurrent.futures import ThreadPoolExecutor
import time
import random
import requests
import pandas as pd
import numpy as np
from typing import Dict, Optional
import json


def send_data_to_service(df: pd.DataFrame, config: Dict, service_name: str) -> Optional[pd.DataFrame]:
    if len(df) == 0:
        return pd.DataFrame()  # Return an empty DataFrame if the input is empty

    data_json = df.to_json(orient="split")
    config_json = json.dumps(config)

    payload = {'data': data_json, 'config': config_json}
    print("Sending {} MB to {}".format(sys.getsizeof(data_json)/1000000, service_name))
    max_retries = 3

    for attempt in range(max_retries):
        try:
            print("Payload keys:", payload.keys())
            print("Payload['data'] sample (truncated):", str(payload['data'])[:500])
            print("Payload['config']:", payload['config'])

            response = requests.post(service_name, json=payload)
            if response.status_code == 200:
                response_data = json.loads(response.text)
                return pd.read_json(StringIO(response_data), orient='split')
            else:
                response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 413:  # Check if error is due to large request
                # Split the DataFrame into two and recurse
                middle_index = len(df) // 2
                first_half = df.iloc[:middle_index]
                second_half = df.iloc[middle_index:]
                result_first_half = send_data_to_service(first_half, config, service_name)
                result_second_half = send_data_to_service(second_half, config, service_name)
                return pd.concat([result_first_half, result_second_half])
            elif attempt < max_retries - 1:
                wait_time = random.randint(2, 6)
                print(f"Error occurred: {e}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            else:
                print(f"Error occurred after {max_retries} attempts: {e}. Sending a warning and continuing...")
                return None
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = random.randint(2, 6)
                print(f"Error occurred: {e}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            else:
                print(f"Error occurred after {max_retries} attempts: {e}. Sending a warning and continuing...")
                return None

    return None 


def prep_df(df: pd.DataFrame):
    """
    Given a dataframe, prepares it for matching by transliterating it and removing vowels from the var_entries
    """
    config = {"trans_flag": False, "phonetic_flag": True, "extended_rules_flag": True, "remove_vowels_flag": False}
    service = "https://mehdi-er-prep-snlwejaxvq-ez.a.run.app/process"
    prep_fields = ['title']
    optional_prep_fields = ['name_parts']

    # filter NaN id values and log a warning
    if df['id'].isnull().sum() > 0:
        print(f"Warning: {df['id'].isnull().sum()} NaN id values found in the dataframe. These will be removed.")
        df = df.dropna(subset=['id'])

    for field in prep_fields:
        if field not in df.columns:
            raise ValueError(f"Dataframe must contain the following fields: {prep_fields} and contains {df.columns}")

    # split the df into two where both contain the ID field and one contains the fields from the prep_fields list and the other contains the rest.
    for field in optional_prep_fields:
        if field in df.columns:
            prep_fields.append(field)
    print(f"OPTIONAL_PREP_FIELDS: {optional_prep_fields} + PREP_FIELDS: {prep_fields}")

    df_to_send = df[['id'] + prep_fields]
    df_to_merge = df.drop(prep_fields, axis=1)

    df_list = np.array_split(df_to_send, math.ceil(len(df) / 1000))

    # create a ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(send_data_to_service, df, config, service) for df in df_list]
        df_list = [future.result() for future in futures if future.result() is not None]

    if not df_list:
        raise RuntimeError("All requests to the preparation service failed.")

    merged_df = pd.concat(df_list)
    
    print("Returned columns from service:", merged_df.columns.tolist())
    merged_df = merged_df.merge(df_to_merge, on='id', how='left')
    
    return merged_df



def write_g2p(data, name):
    print(f"Preparing and saving G2P data for: {name}")
    data = prep_df(data)
    
    """ if name == "LASKI":
        columns = ['name_part_given-name', 'name_part_middle-name', 'name_part_surname' , 'name_part_salutation', 'name_part_patronymic', 'name_part_professional-name', 'name_part_maiden-surname', 'name_part_nisba','name_part_alternative-name', 'name_part_qualifier','name_part_honorific', 'acronym', 'maiden-surname', 'honorific', 'primary-name', 'professional-name', 'salutation', 'teknonym', 'patronymic', 'middle-name', 'qualifier', 'surname', 'given-name', 'alternative-name', 'appellation', 'matronymic', 'nisba']
        data.drop(columns, inplace=True, axis=1) """

    output_file_path = f"datasets/phonetic/{name}_phonetic.csv"
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

    try:
        data.to_csv(output_file_path, index=False, encoding="utf-8")
        print(f"Data saved to {output_file_path}")
    except Exception as e:
        print("Error while saving CSV:", e)