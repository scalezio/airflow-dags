import ast
import itertools

import pandas as pd
from utils.logger import get_logger

from db.tables import RulesModelsTable
from services.model_trainer_service import ModelTrainerService
from math import sqrt


class TrainRuleCombinationsModelService:
    def __init__(self):
        self.logger = get_logger('TrainRuleProbabilityModelService')
        self.model_trainer_service = ModelTrainerService()
        self.attributes = ['Age', 'Size', 'Height', 'BraCupSize', 'JeansSize', 'Weight', 'ShoeSize', 'BodyPartFlaunt',
                           'BodyPartDownplay']

    @staticmethod
    def get_df_by_query(attributes_map: dict, df: pd.DataFrame) -> pd.DataFrame:
        try:
            return df.query(" & ".join([f"({k} == \"{v}\")" for k, v in attributes_map.items()]))
        except:
            return pd.DataFrame()

    @staticmethod
    def calculate_agree_ratio(df: pd.DataFrame) -> float:
        length = df.__len__()
        return ((df[df['status'] == 'AGREE'].__len__() / length) - (1 / (2 * sqrt(length)))) if length > 0 else 0

    @staticmethod
    def get_rules_attributes_df(df):
        df = df[df['attributes'] != {}]
        df = df[['ruleId', 'date', 'attributes', 'status', 'flow']]
        df['attributes'] = df['attributes'].apply(lambda a: ast.literal_eval(a) if isinstance(a, str) else a)
        df_attributes = df['attributes'].apply(pd.Series)
        df = pd.concat([df.drop(['attributes'], axis=1), df_attributes], axis=1)
        df['Height'] = df['Height'].apply(lambda s: s.replace("\"", "") if isinstance(s, str) else None).tolist()
        df = df.where(pd.notnull(df), None)
        return df

    def calculate_rule_combinations(self, attributes_combinations, df_rule) -> list:
        df_comb = pd.DataFrame(
            [{'attributes': {k: v for k, v in zip(att_comb, values_comb)}} for att_comb in attributes_combinations
             for values_comb in itertools.product(*[list(df_rule[att].unique()) for att in att_comb])])
        df_comb['agreeRatio'] = df_comb['attributes'].apply(
            lambda c: self.calculate_agree_ratio(self.get_df_by_query(c, df_rule)))
        df_comb['attributes'].apply(lambda a: a.pop('ruleId', None))
        return df_comb.to_dict('records')

    def train_combinations_model(self, df):
        df = self.get_rules_attributes_df(df)
        attributes_combinations = [['ruleId', att] for att in self.attributes]

        rules_combinations_model = {
            flow: {str(rule_id): self.calculate_rule_combinations(attributes_combinations, df_rule) for
                   rule_id, df_rule in df_flow.groupby(by=['ruleId'])} for flow, df_flow in df.groupby(by=['flow'])}

        return rules_combinations_model

    def train_rule_model(self, from_date, to_date, df=None):
        self.model_trainer_service.train_rule_model(model_name=RulesModelsTable.RULES_COMBINATIONS_MODEL,
                                                    train_model_func=self.train_combinations_model,
                                                    from_date=from_date, to_date=to_date, df=df)
