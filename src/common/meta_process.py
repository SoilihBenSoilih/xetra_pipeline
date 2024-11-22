"""
Methods for processing the meta file
"""
from src.common.constants import MetaProcessFormat
import pandas as pd
from datetime import datetime, timedelta
from src.common.s3 import S3BucketConnector

class MetaProcess():
    """
    class for working with the meta file
    """

    @staticmethod
    def update_meta_file(extract_date_list: list, meta_key: str, bucket: S3BucketConnector):
        """
            Updating meta file with the processed Xetra dates ans todays data as processed date
        """
        df_new = pd.DataFrame(columns=[
            MetaProcessFormat.META_SOURCE_DATE_COL.value,
            MetaProcessFormat.META_PROCESS_COL.value,
        ])

        df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extract_date_list
        df_new[MetaProcessFormat.META_PROCESS_COL.value] = datetime.today().strftime(
            MetaProcessFormat.META_DATE_FORMAT.value
        )
        try:
            df_old = bucket.read_csv_to_df(meta_key)
            df_all = pd.concat([df_old, df_new])

        except:
            df_all = df_new
        
        bucket.write_df_to_s3(df_all, meta_key, MetaProcessFormat.META_FILE_FORMAT.value)

    @staticmethod
    def return_date_list(first_date: str, meta_key: str, bucket: S3BucketConnector):
        """
        Create a list of dates based on the input first_date and the already processed dates in the meta file.
        """
        start = datetime.strptime(first_date, MetaProcessFormat.META_DATE_FORMAT.value).date()
        today = datetime.today().date()

        try:
            df_meta = bucket.read_csv_to_df(meta_key)
            # Extraire et convertir les dates traitées
            processed_dates = set(pd.to_datetime(df_meta[MetaProcessFormat.META_SOURCE_DATE_COL.value]).dt.date)

            # Ajuster la date de début
            if processed_dates:
                start = max(start, max(processed_dates) + timedelta(days=1))

            # Générer toutes les dates entre `start` et aujourd'hui
            dates = [start + timedelta(days=x) for x in range(0, (today - start).days + 1)]

            # Identifier les dates manquantes
            date_missing = set(dates) - processed_dates

            if date_missing:
                min_date = min(date_missing)
                return_min_date = min_date.strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                return_dates = [date.strftime(MetaProcessFormat.META_DATE_FORMAT.value) for date in sorted(date_missing)]
            else:
                return_min_date = datetime(2200, 1, 1).date().strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                return_dates = []
        except Exception as e:
            return_min_date = first_date
            return_dates = [(start + timedelta(days=x)).strftime(MetaProcessFormat.META_DATE_FORMAT.value) for x in range(0, (today - start).days + 1)]

        return return_min_date, return_dates
