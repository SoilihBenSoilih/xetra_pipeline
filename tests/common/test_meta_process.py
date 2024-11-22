"""TestS3BucketConnectorMethods"""
import unittest
from datetime import datetime, timedelta

import boto3
from decouple import config
from moto import mock_s3

from src.common.s3 import S3BucketConnector
from src.common.meta_process import MetaProcess
from src.common.constants import MetaProcessFormat



class TestS3BucketConnectorMethods(unittest.TestCase):
    """
    Testing the S3BucketConnector class.
    """

    def setUp(self):
        """
        Setting up the environment
        """
        # mocking s3 connection start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()

        # Defining the class arguments
        self.s3_access_key = config('AWS_ACCESS_KEY')
        self.s3_secret_key = config('AWS_SECRET_KEY')
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'

        # Creating a bucket on the mocket s3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                                  CreateBucketConfiguration={
                                      'LocationConstraint': 'eu-central-1'})
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)

        # Creating a testing instance
        self.s3_bucket_conn = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name)

    def tearDown(self):
        # mocking s3 connection stop
        self.mock_s3.stop()

    def test_update_meta_file_new_file(self):
        """
        Test update_meta_file when the meta file does not exist.
        Should create a new file with the provided extract dates.
        """
        # Test setup
        extract_date_list = ['2024-01-01', '2024-01-02']
        meta_key = "meta.csv"
        
        # Execute the method
        MetaProcess.update_meta_file(extract_date_list, meta_key, self.s3_bucket_conn)
        
        # Verify the result
        obj = self.s3_bucket.Object(key=meta_key).get()
        csv_content = obj['Body'].read().decode('utf-8')
        
        expected_content = f"{MetaProcessFormat.META_SOURCE_DATE_COL.value},{MetaProcessFormat.META_PROCESS_COL.value}\n"
        expected_content += f"2024-01-01,{datetime.today().strftime(MetaProcessFormat.META_DATE_FORMAT.value)}\n"
        expected_content += f"2024-01-02,{datetime.today().strftime(MetaProcessFormat.META_DATE_FORMAT.value)}\n"
        
        self.assertEqual(csv_content, expected_content)

    def test_update_meta_file_existing_file(self):
        """
        Test update_meta_file when the meta file already exists.
        Should append the new extract dates to the existing file.
        """
        # Test setup
        existing_content = f"{MetaProcessFormat.META_SOURCE_DATE_COL.value},{MetaProcessFormat.META_PROCESS_COL.value}\n"
        existing_content += f"2023-12-31,{datetime.today().strftime(MetaProcessFormat.META_DATE_FORMAT.value)}\n"
        meta_key = "meta.csv"
        
        # Upload the existing file to the mocked S3 bucket
        self.s3_bucket.put_object(Body=existing_content, Key=meta_key)
        
        extract_date_list = ['2024-01-01', '2024-01-02']
        
        # Execute the method
        MetaProcess.update_meta_file(extract_date_list, meta_key, self.s3_bucket_conn)
        
        # Verify the result
        obj = self.s3_bucket.Object(key=meta_key).get()
        csv_content = obj['Body'].read().decode('utf-8')
        
        expected_content = existing_content
        expected_content += f"2024-01-01,{datetime.today().strftime(MetaProcessFormat.META_DATE_FORMAT.value)}\n"
        expected_content += f"2024-01-02,{datetime.today().strftime(MetaProcessFormat.META_DATE_FORMAT.value)}\n"
        
        self.assertEqual(csv_content, expected_content)

    def test_update_meta_file_empty_list(self):
        """
        Test update_meta_file with an empty extract_date_list.
        Should not modify the existing meta file or create a new one.
        """
        # Test setup
        extract_date_list = []
        meta_key = "meta.csv"
        
        # Execute the method
        MetaProcess.update_meta_file(extract_date_list, meta_key, self.s3_bucket_conn)
        
        # Check that no object was created in S3
        objects = list(self.s3_bucket.objects.filter(Prefix=meta_key))
        self.assertEqual(len(objects), 0)

    def test_update_meta_file_invalid_key(self):
        """
        Test update_meta_file with an invalid key.
        Should raise an exception during S3 read/write operations.
        """
        # Test setup
        extract_date_list = ['2024-01-01']
        invalid_meta_key = "invalid/meta.csv"
        
        # Mocking S3 exception for the read_csv_to_df method
        with self.assertRaises(Exception) as ctx:
            MetaProcess.update_meta_file(extract_date_list, invalid_meta_key, self.s3_bucket_conn)
            self.assertTrue(isinstance(ctx.exception, Exception))
    
    def test_return_date_list_no_meta_file(self):
        """
        Test return_date_list when no meta file exists.
        Should return a full list of dates starting from the first_date.
        """
        # Test setup
        first_date = '2024-01-01'
        meta_key = "meta.csv"

        # Execute the method
        return_min_date, return_dates = MetaProcess.return_date_list(first_date, meta_key, self.s3_bucket_conn)

        # Verify results
        expected_dates = [
            (datetime.strptime(first_date, MetaProcessFormat.META_DATE_FORMAT.value).date() + timedelta(days=x)).strftime(
                MetaProcessFormat.META_DATE_FORMAT.value)
            for x in range((datetime.today().date() - datetime.strptime(first_date, MetaProcessFormat.META_DATE_FORMAT.value).date()).days + 1)
        ]
        self.assertEqual(return_min_date, first_date)
        self.assertEqual(return_dates, expected_dates)

    def test_return_date_list_meta_file_exists(self):
        """
        Test return_date_list when meta file exists and contains valid data.
        Should return only missing dates after the last processed date in the meta file.
        """
        # Test setup
        first_date = '2024-01-01'
        meta_key = "meta.csv"
        existing_content = f"{MetaProcessFormat.META_SOURCE_DATE_COL.value},{MetaProcessFormat.META_PROCESS_COL.value}\n"
        existing_content += f"2024-01-01,{datetime.today().strftime(MetaProcessFormat.META_DATE_FORMAT.value)}\n"
        self.s3_bucket.put_object(Body=existing_content, Key=meta_key)

        # Execute the method
        return_min_date, return_dates = MetaProcess.return_date_list(first_date, meta_key, self.s3_bucket_conn)

        # Verify results
        expected_dates = [
            (datetime.strptime(first_date, MetaProcessFormat.META_DATE_FORMAT.value).date() + timedelta(days=x)).strftime(
                MetaProcessFormat.META_DATE_FORMAT.value)
            for x in range(1, (datetime.today().date() - datetime.strptime(first_date, MetaProcessFormat.META_DATE_FORMAT.value).date()).days + 1)
        ]
        self.assertEqual(return_min_date, "2024-01-02")
        self.assertEqual(return_dates, expected_dates)

    def test_return_date_list_meta_file_corrupted(self):
        """
        Test return_date_list when the meta file is corrupted.
        Should return a full list of dates starting from the first_date.
        """
        # Test setup
        first_date = '2024-01-01'
        meta_key = "meta.csv"
        corrupted_content = "INVALID DATA"
        self.s3_bucket.put_object(Body=corrupted_content, Key=meta_key)

        # Execute the method
        return_min_date, return_dates = MetaProcess.return_date_list(first_date, meta_key, self.s3_bucket_conn)

        # Verify results
        expected_dates = [
            (datetime.strptime(first_date, MetaProcessFormat.META_DATE_FORMAT.value).date() + timedelta(days=x)).strftime(
                MetaProcessFormat.META_DATE_FORMAT.value)
            for x in range((datetime.today().date() - datetime.strptime(first_date, MetaProcessFormat.META_DATE_FORMAT.value).date()).days + 1)
        ]
        self.assertEqual(return_min_date, first_date)
        self.assertEqual(return_dates, expected_dates)

    def test_return_date_list_empty_dates(self):
        """
        Test return_date_list with an empty meta file.
        Should return all dates from the first_date.
        """
        # Test setup
        first_date = '2024-01-01'
        meta_key = "meta.csv"
        empty_content = f"{MetaProcessFormat.META_SOURCE_DATE_COL.value},{MetaProcessFormat.META_PROCESS_COL.value}\n"
        self.s3_bucket.put_object(Body=empty_content, Key=meta_key)

        # Execute the method
        return_min_date, return_dates = MetaProcess.return_date_list(first_date, meta_key, self.s3_bucket_conn)

        # Verify results
        expected_dates = [
            (datetime.strptime(first_date, MetaProcessFormat.META_DATE_FORMAT.value).date() + timedelta(days=x)).strftime(
                MetaProcessFormat.META_DATE_FORMAT.value)
            for x in range((datetime.today().date() - datetime.strptime(first_date, MetaProcessFormat.META_DATE_FORMAT.value).date()).days + 1)
        ]
        self.assertEqual(return_min_date, first_date)
        self.assertEqual(return_dates, expected_dates)


if __name__ == '__main__':
    unittest.main()