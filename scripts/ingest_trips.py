import requests
import pandas
import io
import boto3
from zipfile import ZipFile

url='https://montreal.bixi.com/c/bixi/file_db/data_all.file/BixiMontrealRentals2019.zip'
response = requests.get(url, stream=True)

z = ZipFile(io.BytesIO(response.content))

text_files = z.infolist()

for text_file in text_files:
    print(text_file.filename)
    if text_file.filename == 'OD_2019-07.csv':
        df = pandas.read_csv(z.open(text_file.filename))

## Use this code if you want to bifurcate the trips in 5 minute interval period
mask = (df['start_date'] > '2019-07-01 00:00:00') & (df['start_date'] < '2019-07-01 00:05:00')
df_interval = df.loc[mask]

## Logic for putting the files in S3