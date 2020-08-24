import io
import os
import pickle

import pandas as pd

from db.tables import RulesModelsTable
from services.dynamo_db_service import DynamoDBService
from services.rules_events_parser import RulesEventsParser
from services.s3_service import S3Service
from utils.date_utils import parse_date
from utils.logger import get_logger


class ModelTrainerService:
    def __init__(self):
        env = os.getenv('ENV', 'prod')
        account = os.getenv('AWS_ACCOUNT', '369120691906')

        self.logger = get_logger('ModelTrainerService')

        self.data_bucket_name = os.getenv('DATA_BUCKET_NAME', f'rules-models-{env}-data-{account}')
        self.results_bucket_name = os.getenv('RESULTS_BUCKET_NAME', f'rules-models-{env}-results-{account}')
        self.event_name = 'UserRatedRule'
        self.s3_service = S3Service()
        self.rules_events_parser = RulesEventsParser()
        self.dynamo_db_service = DynamoDBService(RulesModelsTable.TABLE_NAME)

    @staticmethod
    def is_file_valid(file_name, from_date, to_date) -> bool:
        try:
            dates = str(file_name.split('/')[1]).replace('raw_events_', '').replace('.csv', '').split('_')
            return bool(from_date <= dates[0] and dates[1] <= to_date)
        except:
            return False

    def get_df_rules_events(self, dfs):
        df = pd.concat(dfs).drop_duplicates('timestamp')
        df = self.rules_events_parser.parse_events(df)
        return df

    @staticmethod
    def load_files_to_dfs(csv_files):
        return [pd.read_csv(io.BytesIO(csv_file['Body'].read())) for csv_file in csv_files]

    def download_files(self, files_names):
        return [self.s3_service.s3_client.get_object(Bucket=self.data_bucket_name, Key=file_name) for file_name in
                files_names]

    def get_valid_files_names(self, files_iter, from_date, to_date):
        files_names = [file_['Key'] for file_ in files_iter if self.is_file_valid(file_['Key'], from_date, to_date)]
        return files_names

    def download_data(self, from_date_str, to_date_str):
        self.logger.info('Start get valid files names')
        files_names = [f'{self.event_name}/raw_events_{from_date_str}_{to_date_str}.csv']
        self.logger.info('Done get valid files names')

        self.logger.info(f'Start download {len(files_names)} files')
        csv_files = self.download_files(files_names)
        self.logger.info(f'Done download {len(csv_files)} files')

        self.logger.info('Start load files to dfs')
        dfs = self.load_files_to_dfs(csv_files)
        self.logger.info('Done load files to dfs')

        if dfs:
            self.logger.info('Start get df rules events')
            df = self.get_df_rules_events(dfs)
            self.logger.info('Done get df rules events')
            return df

        return pd.DataFrame()

    def train_rule_model(self, model_name: str, train_model_func: callable, from_date: str, to_date: str, df=None):
        df = df if isinstance(df, pd.DataFrame) else pd.DataFrame()

        to_date_str = parse_date(to_date).strftime('%Y-%m-%d')
        from_date_str = parse_date(from_date).strftime('%Y-%m-%d')
        df = self.download_data(from_date_str, to_date_str) if df.empty else df

        if not df.empty:
            self.logger.info('Start train model')
            model = train_model_func(df)
            self.logger.info('Done train model')

            self.logger.info('Start saving model')
            model_pickle = pickle.dumps(model)
            self.s3_service.s3_client.put_object(Bucket=self.results_bucket_name, Key=model_name, Body=model_pickle)
            self.s3_service.s3_client.put_object(Bucket=self.results_bucket_name,
                                                 Key=f'history/{model_name}/{model_name}_{from_date_str}_{to_date_str}',
                                                 Body=model_pickle)
            self.logger.info('Done saving model')
