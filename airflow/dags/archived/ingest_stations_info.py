import json
import requests
import pandas as pd
import pyarrow

station_info_api = 'https://api-core.bixi.com/gbfs/en/station_information.json'
station_info_response = requests.get(station_info_api)
station_info = station_info_response.json()
last_updated = station_info['last_updated']
bootstrap_vars = 
	{
		"station_id": "-1",
		"external_id": "bootstrap",
		"name": "bootstrap",
		"short_name": "-1",
		"lat": 0.0,
		"lon":  0.0,
		"rental_methods_0": "bootstrap",
		"rental_methods_1": "bootstrap",
		"capacity": 0,
		"electric_bike_surcharge_waiver": false,
		"eightd_has_key_dispenser": false,
		"eightd_station_services_0_id": "bootstrap",
		"eightd_station_services_0_service_type": "bootstrap",
		"eightd_station_services_0_bikes_availability": "bootstrap",
		"eightd_station_services_0_docks_availability": "bootstrap",
		"eightd_station_services_0_name": "bootstrap",
		"eightd_station_services_0_description": "bootstrap",
		"eightd_station_services_0_schedule_description": "bootstrap",
		"eightd_station_services_0_link_for_more_info": "bootstrap",
		"has_kiosk": true,
		"last_updated": 0000000000
	}
bootstrap_df = pd.DataFrame.from_dict(bootstrap_vars)
station_info_df = pd.DataFrame.from_dict(station_info['data']['stations'])
station_info_df['last_updated'] = last_updated
df_union = pd.concat([bootstrap_df, station_info_df], ignore_index = True)
station_info_df.to_parquet(f'station_info_{last_updated}')


