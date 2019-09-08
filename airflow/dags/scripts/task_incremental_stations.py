import json
import requests
import pandas as pd
import boto3
from datetime import datetime
from flatten_json import flatten
from io import BytesIO, StringIO
from airflow.contrib.hooks.aws_hook import AwsHook

def get_aws_config(conn_id):

	aws_hook = AwsHook(conn_id)
	credentials = aws_hook.get_credentials()
	return credentials

def dataframe_to_s3(s3_client, input_datafame, bucket_name, file_info, format):

	if format == 'parquet':
		out_buffer = BytesIO()
		input_datafame.to_parquet(out_buffer, index=False)

	elif format == 'csv':
		out_buffer = StringIO()
		input_datafame.to_parquet(out_buffer, index=False)

	else:
		print("Undefined or No format defined")

	filename = file_info[0]
	filepath = file_info[1]

	s3_client.put_object(Bucket=bucket_name, Key=filepath, Body=out_buffer.getvalue())
	print(f'{filename} successfully loaded to s3')

endpoint = 'station_status'

def ingest_stations(**kwargs):

	endpoint = kwargs['endpoint']

	station_status_api = f'https://api-core.bixi.com/gbfs/en/{endpoint}.json'

	try:
		station_status_response = requests.get(station_status_api)
	except:
		print("issue with python calling api endpoint")

	station_status = station_status_response.json()
	last_updated = station_status['last_updated']

	test_item = station_status['data']['stations'][0]

	flattened_station_status = [flatten(d) for d in station_status['data']['stations']]

	df = pd.DataFrame(flattened_station_status)

	df['last_updated'] = last_updated

	s3_client = boto3.client('s3', aws_access_key_id=get_aws_config('aws_credentials')[0], aws_secret_access_key=get_aws_config('aws_credentials')[1])

	start_date = datetime.now()

	year = start_date.strftime("%Y")
	month = start_date.strftime("%m")
	day = start_date.strftime("%d")
	hour = start_date.strftime("%H")

	minute = int(start_date.strftime("%M"))

	if (minute//5)*5 == 0:
		min_bucket = '00'
	elif (minute//5)*5 == 5:
		min_bucket = '05'
	else:
		min_bucket = str((minute//5)*5)

	filename = f'{endpoint}_{last_updated}.parquet'
	filepath = f'station/{endpoint}/{year}/{month}/{day}/{hour}/{min_bucket}/{filename}'
	bucket_name = 'bixi.qc.staged'
	file_info = (filename, filepath)
	dataframe_to_s3(s3_client, df, bucket_name, file_info, 'parquet')

if __name__ == '__main__':
	
	kwargs = {'endpoint':'station_status'}
