""" Connector and methods accessing s3 """
from decouple import config
import boto3
from typing import Union
import logging
import pandas as pd
from io import StringIO, BytesIO
from src.common.constants import S3FileTypes
from src.common.custom_exceptions import WrongFormatException


class S3BucketConnector():
    """
        Class for interacting with s3 bucket
    """
    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str) -> None:
        """
        Constructor for S3BucketConnector

        :param access_key: access key for accessing S3
        :param secret_key: secret key for accessing S3
        :param endpoint_url: endpoint url to S3
        :param bucket: S3 bucket name
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self._s3 = boto3.resource(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        self._bucket = self._s3.Bucket(bucket)

    def list_files_in_prefix(self, prefix: str):
        """
        listing all files with a prefix on the S3 bucket

        :param prefix: prefix on the S3 bucket that should be filtered with

        returns:
          files: list of all the file names containing the prefix in the key
        """
        return [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]

    def read_csv_to_df(self, key, sep=",", decoding='utf-8'):
        self._logger.info("Reading file %s/%s/%s/", self.endpoint_url, self._bucket.name, key)
        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(decoding)
        data = StringIO(csv_obj)
        return pd.read_csv(data, delimiter=sep, skip_blank_lines=True, skipinitialspace=True)

    def write_df_to_s3(self, data_frame:pd.DataFrame, key: str, file_format: str):
        """
        writing a Pandas Dataframe to S3
        supported formats: .csv, .parquet
        """
        formats = set(("csv", "parquet"))
        if file_format not in formats:
            message = f"unsupported format, only support: {formats}"
            self._logger.info(message)
            raise WrongFormatException(message)
        if data_frame.empty:
            self._logger.info("Dataframe is empty")
            return None
        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        
    def __put_object(self, out_buffer: Union[StringIO, BytesIO], key: str):
        """
        Put an object to s3
        """
        self._logger.info("Writing file %s/%s/%s/", self.endpoint_url, self._bucket.name, key)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True

