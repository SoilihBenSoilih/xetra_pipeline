"""TestS3BucketConnectorMethods"""
import os
import pandas as pd
import unittest
from datetime import datetime

import boto3
from decouple import config
from moto import mock_s3

from src.common.s3 import S3BucketConnector
from src.common.meta_process import MetaProcess
from src.common.constants import MetaProcessFormat
from src.common.custom_exceptions import WrongFormatException
from io import StringIO, BytesIO



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

    def test_list_files_in_prefix_ok(self):
        """
        Tests the list_files_in_prefix method for getting
        the 2 file keys as list on the mocket s3 bucket
        """
        # Expected results
        prefix_exp = 'prefix/'
        key1_exp = f'{prefix_exp}test1.csv'
        key2_exp = f'{prefix_exp}test2.csv'

        # Test init
        csv_content = """col1,col2
        valA,valB"""
        self.s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_exp)

        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
        
        # Test after method execution
        self.assertEqual(len(list_result), 2)
        self.assertIn(key1_exp, list_result)
        self.assertIn(key2_exp, list_result)
        
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key1_exp
                    },
                    {
                        'Key': key2_exp
                    },
                ]
            }
        )


    def test_list_files_in_prefix_wrong_prefix(self):
        """
        Tests the list_files_in_prefix method in case of a
        wrong or not existing prefix
        """
        # Test init
        prefix = 'no-prefix/'

        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix)
        
        # Test after method execution
        self.assertTrue(not list_result)
    
    def test_read_csv_to_df(self):
        """
        Tests the read_csv_to_df method for reading CSV content into a DataFrame.
        """
        # Test setup
        csv_content = """col1,col2
        val1,val2
        val3,val4"""
        key = 'test.csv'

        # Load the CSV file into the mocked S3 bucket
        self.s3_bucket.put_object(Body=csv_content, Key=key)

        # Execute the method
        df_result = self.s3_bucket_conn.read_csv_to_df(key)

        # Verify the results
        self.assertEqual(df_result.shape, (2, 2))  # Check the number of rows and columns
        self.assertListEqual(df_result.columns.tolist(), ['col1', 'col2'])  # Check column names
        self.assertEqual(df_result.iloc[0, 0], 'val1')  # Check the first value
        self.assertEqual(df_result.iloc[1, 1], 'val4')  # Check the last value

    def test_write_df_to_s3_empty_df(self):
        """
        Tests the write_df_to_s3 method with an empty DataFrame.
        Should return None and not write any object to S3.
        """
        empty_df = pd.DataFrame()
        key = "empty.csv"
        
        result = self.s3_bucket_conn.write_df_to_s3(empty_df, key, file_format="csv")
        
        # Check that result is None and no object is created
        self.assertIsNone(result)
        objects = list(self.s3_bucket.objects.filter(Prefix=key))
        self.assertEqual(len(objects), 0)

    def test_write_df_to_s3_unsupported_format(self):
        """
        Tests the write_df_to_s3 method with an unsupported file format.
        Should raise WrongFormatException.
        """
        df = pd.DataFrame({"col1": ["val1"], "col2": ["val2"]})
        key = "test.unsupported"
        
        with self.assertRaises(WrongFormatException):
            self.s3_bucket_conn.write_df_to_s3(df, key, file_format="unsupported")
    
    def test_write_df_to_s3_csv(self):
        """
        Tests the write_df_to_s3 method by writing a CSV file to S3.
        Checks if the CSV file content is correct.
        """
        df = pd.DataFrame({"col1": ["val1", "val2"], "col2": ["val3", "val4"]})
        key = "test.csv"
        
        result = self.s3_bucket_conn.write_df_to_s3(df, key, file_format="csv")
        
        # Check that the object was created
        self.assertTrue(result)
        obj = self.s3_bucket.Object(key=key).get()
        csv_content = obj['Body'].read().decode('utf-8')
        expected_content = "col1,col2\nval1,val3\nval2,val4\n"
        
        self.assertEqual(csv_content, expected_content)

    def test_write_df_to_s3_parquet(self):
        """
        Tests the write_df_to_s3 method by writing a Parquet file to S3.
        Checks if the Parquet file is stored correctly.
        """
        df = pd.DataFrame({"col1": ["val1", "val2"], "col2": ["val3", "val4"]})
        key = "test.parquet"
        
        result = self.s3_bucket_conn.write_df_to_s3(df, key, file_format="parquet")
        
        # Check that the object was created
        self.assertTrue(result)
        obj = self.s3_bucket.Object(key=key).get()
        
        # Read the Parquet file from S3
        parquet_content = BytesIO(obj['Body'].read())
        df_from_s3 = pd.read_parquet(parquet_content)
        
        # Verify the DataFrame content
        pd.testing.assert_frame_equal(df, df_from_s3)

if __name__ == '__main__':
    unittest.main()