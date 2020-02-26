import luigi
import requests
import pandas as pd
import pyarrow.parquet as pq
from s3fs import S3FileSystem
from luigi.format import MixedUnicodeBytesFormat


class DownloadDatasetTask(luigi.Task):
    """Downloads Yellow Taxi dataset for 2019-03"""

    def run(self):
        url = f'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-03.csv'
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with self.output().open(mode='w') as f:
            for chunk in response.iter_lines():
                f.write('{}\n'.format(chunk.decode('utf-8')))

    def output(self):
        return luigi.LocalTarget(
            'yellow_tripdata_2019-03.csv', format=MixedUnicodeBytesFormat()
        )


class ConvertToParquetTask(luigi.Task):
    """Converts Yellow Taxi csv to parquet with filtering"""

    def requires(self):
        return DownloadDatasetTask()

    def run(self):
        df = pd.read_csv(self.input().open())
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df = df[
            (df['tpep_pickup_datetime'] >= '2019-03-01') &
            (df['tpep_pickup_datetime'] < '2019-04-01')
        ]
        df['pickup_date'] = df['tpep_pickup_datetime'].dt.strftime('%Y-%m-%d')
        df.to_parquet(self.output().path)

    def output(self):
        return luigi.LocalTarget(
            'yellow_tripdata_2019-03.parquet', format=MixedUnicodeBytesFormat()
        )


class UploadToS3Task(luigi.Task):
    """Uploads partitioned data by pickup_date to Amazon S3"""
    def requires(self):
        return ConvertToParquetTask()

    def run(self):
        table = pq.read_table(self.input().path)
        pq.write_to_dataset(
            table,
            root_path='s3://aws-meetup-almaty/yellow-taxi-ds',
            partition_cols=['pickup_date'],
            filesystem=S3FileSystem(),
        )
