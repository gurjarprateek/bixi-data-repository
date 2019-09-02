import json
import requests
import pandas as pd
import pyarrow

station_info_api = 'https://api-core.bixi.com/gbfs/en/station_information.json'
station_info_response = requests.get(station_info_api)
station_info = station_info_response.json()
last_updated = station_info['last_updated']
station_info_df = pd.DataFrame.from_dict(station_info['data']['stations'])
station_info_df['last_updated'] = last_updated
station_info_df.to_parquet(f'station_info_{last_updated}')