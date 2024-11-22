"""Running the Xetra ETL application"""
import yaml
import argparse
import logging
import logging.config
from decouple import config

from src.common.s3 import S3BucketConnector
from src.transformers.xetra_transformer import XetraETL, XetraSourceConfig, XetraTargetConfig


def main():
    """
        entry point to run the ETL job
    """
    # parse YAML file
    config_path = "./configs/xetra_report1_config.yaml"
    yml_config = yaml.safe_load(open(config_path))
    # parser = argparse.ArgumentParser(description='Run the Xetra ETL Job.')
    # parser.add_argument('config', help='A configuration file in YAML format.')
    # args = parser.parse_args()
    # yml_config = yaml.safe_load(open(args.config))
    
    # configure logging
    log_config = yml_config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)

    # reading s3 configuraation
    s3_config = yml_config['s3']

    # create the s3 instances
    s3_bucket_src = S3BucketConnector(
        access_key=config("AWS_ACCESS_KEY"),
        secret_key=config("AWS_SECRET_KEY"),
        endpoint_url=s3_config["src_endpoint_url"],
        bucket=config('S3_BUCKET')
    )
    s3_bucket_trg = S3BucketConnector(
        access_key=config("AWS_ACCESS_KEY"),
        secret_key=config("AWS_SECRET_KEY"),
        endpoint_url=s3_config["src_endpoint_url"],
        bucket=config('S3_BUCKET')
    )

    # reading source configuration
    source_config = XetraSourceConfig(**yml_config['source'])
    
    # reading target configuration
    target_config = XetraTargetConfig(**yml_config['target'])

    # reading meta file configuration
    meta_config = yml_config['meta']

    # creating XetraETL class
    logger = logging.getLogger(__name__)
    logger.info('Xetra ETL job started.')
    xetra_etl = XetraETL(
        s3_bucket_src, 
        s3_bucket_trg,
        meta_config['meta_key'], 
        source_config, 
        target_config
    )
    
    # running etl job for xetra report 1
    xetra_etl.etl_report1()
    logger.info('Xetra ETL job finished.')

if __name__ == '__main__':
    main()


