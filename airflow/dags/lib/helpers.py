import requests, pandas, boto3, os, configparser, datetime, logging
from io import BytesIO, StringIO
from zipfile import ZipFile
from airflow.contrib.hooks.aws_hook import AwsHook

def get_aws_config(conn_id):

	aws_hook = AwsHook(conn_id)
	credentials = aws_hook.get_credentials()
	return credentials

def get_aws_config_deprecated(profile, key):

	config = configparser.ConfigParser()
	config.read(f'{os.environ["AWS_CREDS"]}/credentials')
	return config[profile][key]

def download_extract(url):

	print(f'Downloading dataset from {url}')
	print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')

	response = requests.get(url, stream=True)
	print('Download Complete')
	print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')

	print('Unzipping response to byte stream')
	print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')

	z = ZipFile(BytesIO(response.content))
	print('Unzipping response Complete')
	print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')

	return z

def dataframe_to_s3(s3_client, input_datafame, bucket_name, file_info):

	csv_buffer = StringIO()
	input_datafame.to_csv(csv_buffer, index=False)

	filename = file_info[0]
	filepath = file_info[1]

	s3_client.put_object(Bucket=bucket_name, Key=filepath, Body=csv_buffer.getvalue())
	print(f'{filename} successfully loaded to s3')
	
def get_file_info(start_date):

	year = start_date.strftime("%Y")
	month = start_date.strftime("%m")
	day = start_date.strftime("%d")
	hour = start_date.strftime("%H")

	filename = start_date.strftime("%Y%m%d_%H-%M-%S")+'.csv'
	filepath = f'{year}/{month}/{day}/{hour}/{filename}'
	file_info = (filename, filepath)
	return file_info

def set_logging():
	logging.getLogger().setLevel(logging.INFO)
	logging.basicConfig(format='%(asctime)s %(levelname)-s %(message)s',level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

def load_historical(**kwargs):

	url = kwargs['url']
	bucket_name = kwargs['bucket_name']
	load = kwargs['load']

	s3_client = boto3.client('s3', aws_access_key_id=get_aws_config('aws_credentials')[0], aws_secret_access_key=get_aws_config('aws_credentials')[1])

	z = download_extract(url)

	text_files = z.infolist()

	if load=='trips':

		for text_file in text_files:

			if 'od' in text_file.filename.lower():
				print(f'starting load for {text_file.filename}')
				start_date = datetime.datetime.strptime(text_file.filename.split('_')[1].split('.')[0], "%Y-%m")
				df = pandas.read_csv(z.open(text_file.filename))
				year = start_date.strftime("%Y")
				month = start_date.strftime("%m")
				filepath = f'trips/{year}/{month}/{text_file.filename}'
				file_info = (text_file.filename, filepath)
				dataframe_to_s3(s3_client, df, bucket_name, file_info)

	elif load=='stations':

		for text_file in text_files:

			if 'station' in text_file.filename.lower():
				print(f'starting load for {text_file.filename}')
				start_date = datetime.datetime.strptime(text_file.filename.split('_')[1].split('.')[0], "%Y")
				df = pandas.read_csv(z.open(text_file.filename))
				year = start_date.strftime("%Y")
				filepath = f'station/{year}/{text_file.filename}'
				file_info = (text_file.filename, filepath)
				dataframe_to_s3(s3_client, df, bucket_name, file_info)


def dataframe_to_s3_loader():

	for text_file in text_files:
		#logging.info(text_file.filename)
		if text_file.filename == 'OD_2019-07.csv':
			print(f'starting load for {text_file.filename}')
			df = pandas.read_csv(z.open(text_file.filename))

			# start_date = datetime.datetime.strptime(df.get_value(0,'start_date'), "%Y-%m-%d %H:%M:%S")
			# for i in df.index:
			# 	end_date = start_date + datetime.timedelta(0, 300)
			# 	if end_date > datetime.datetime.strptime(df.get_value(i,'start_date'), "%Y-%m-%d %H:%M:%S"):
			# 		pass
			# 	else:
			# 		mask = (df['start_date'] > start_date.strftime("%Y-%m-%d %H:%M:%S")) & (df['start_date'] < end_date.strftime("%Y-%m-%d %H:%M:%S"))
			# 		df_interval = df.loc[mask]
			# 		get_filepath(start_date)
			# 		dataframe_to_s3(s3_client, df_interval, 'bixi.qc.raw', filepath)
			# 		start_date = end_date

			# start_date = df['start_date'][0]
			# for i in df.index:
			# 	end_date = datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(0, 300)
			# 	if end_date > datetime.datetime.strptime(df['start_date'][i], "%Y-%m-%d %H:%M:%S"):
			# 		pass
			# 	else:
			# 		mask = (df['start_date'] < end_date.strftime("%Y-%m-%d %H:%M:%S"))
			# 		df_interval = df.loc[mask]
			# 		file_info = get_file_info(datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S"))
			# 		dataframe_to_s3(s3_client, df_interval, 'bixi.qc.raw', file_info)
			# 		start_date = end_date.strftime("%Y-%m-%d %H:%M:%S")

			start_date = datetime.datetime(2019, 7, 1)
			i = 0
			## Use this code if you want to bifurcate the trips in 5 minute interval period
			for i in range(0, 8928):
				end_date = start_date + datetime.timedelta(0, 300)
				mask = (df['start_date'] < end_date.strftime("%Y-%m-%d %H:%M:%S"))
				df_interval = df.loc[mask]
				file_info = get_file_info(start_date)
				dataframe_to_s3(s3_client, df_interval, 'bixi.qc.raw', file_info)
				start_date = end_date