import json
import requests
import pandas as pd
import pyarrow

station_status_api = 'https://api-core.bixi.com/gbfs/en/station_status.json'
station_status_response = requests.get(station_status_api)

station_status = station_status_response.json()
last_updated = station_status['last_updated']

station_status_df = pd.DataFrame.from_dict(station_status['data']['stations'])
station_status_df['last_updated'] = last_updated
#station_status_df['eightd_active_services_station_id']

#station_status_df['station_id'] = station_status_df['eightd_active_station_services']['id']
station_status_df.drop(columns='eightd_active_station_services')

station_status_df.to_parquet(f'station_status_{last_updated}')