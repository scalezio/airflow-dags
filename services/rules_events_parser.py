from datetime import datetime
from ast import literal_eval
import pandas as pd

from services.attributes_mapping_service import AttributesMappingService
from services.vector_service import VectorService
from services.ede_rules_service import EdeRulesService
from utils.logger import timing
from utils.singelton import SingletonDecorator


@SingletonDecorator
class RulesEventsParser:
    def __init__(self):
        self.ede_rules_service = EdeRulesService()
        self.vector_service = VectorService()
        attributes_mapping_service = AttributesMappingService()
        mappings = attributes_mapping_service.provide_attributes_mappings()
        self.mapping = {k: v for mapping in mappings.values() for k, v in mapping.items()}

    @timing()
    def validate_rules(self, df):
        rules_dict = self.ede_rules_service.get_rules(list(df['flow'].unique()))
        df['isValid'] = df.apply(lambda d: bool(str(d['ruleId']) in rules_dict.get(d['flow'], [])), axis=1)
        df = df[df['isValid']]
        df = df.drop(columns=['isValid'], axis=1)
        return df

    @staticmethod
    def flatten_dict_values(attributes):
        return {k: v[0] if isinstance(v, list) and v else v for k, v in attributes.items()} if attributes else {}

    def clean_attributes(self, attributes, rule_attributes):
        [attributes.pop(att, None) for att in rule_attributes.keys()]
        return {k: v for k, v in attributes.items() if k in self.mapping.keys() and v in self.mapping.get(k).keys()}

    @staticmethod
    def get_rule(rules, rule):
        return rule if rule and isinstance(rule, dict) else rules[0] if rules else None

    @staticmethod
    def get_status(rule, rate):
        return rate if rate and isinstance(rate, str) else rule.get('status')

    @staticmethod
    def get_rule_id(rule):
        try:
            return int(rule.get('ruleId'))
        except Exception:
            return 0

    @staticmethod
    def get_rule_position(rule, position):
        try:
            return int(position)
        except Exception:
            return int(rule.get('position') or rule.get('order'))

    @staticmethod
    def get_date(date_string: str):
        try:
            return datetime.strptime(date_string, '%Y-%m-%d').strftime('%Y-%m-%d')
        except:
            try:
                return datetime.strptime(date_string.split('T')[0], '%Y-%m-%d').strftime('%Y-%m-%d')
            except:
                try:
                    return datetime.strptime(date_string, '%d/%m/%Y').strftime('%Y-%m-%d')
                except:
                    return None

    @timing()
    def parse_events(self, df, is_split_agree_not_agree=False, is_add_vector=False):
        columns = ['userId', 'flow', 'ruleId', 'rulePosition', 'status', 'ruleInputAttributes',
                   'ruleOutputAttributes', 'attributes', 'date', 'timestamp', 'isRandom']
        df = df.dropna(subset=['payload'])

        df['date'] = df['timestamp'].apply(self.get_date)
        df = df[df['date'].notnull()]
        df['payload'] = df['payload'].apply(lambda p: literal_eval(p) if isinstance(p, str) else p)
        df['data'] = df.apply(lambda d: {**d['payload'], 'date': d['date'], 'timestamp': d['timestamp']}, axis=1)
        df = pd.DataFrame(df['data'].tolist())

        # df['rule'] = df.apply(lambda d: self.get_rule(d['rules'], d['rule']), axis=1)
        df['status'] = df.apply(lambda d: self.get_status(d['rule'], d['rate']), axis=1)
        df['flow'] = df['rule'].apply(lambda r: r.get('flow'))
        df['ruleInputAttributes'] = df['rule'].apply(lambda rule: rule.get('inputAttributes') if rule else {})
        df['ruleOutputAttributes'] = df['rule'].apply(lambda rule: rule.get('outputAttributes') if rule else {})
        df['ruleOutputAttributes'] = df['ruleOutputAttributes'].apply(
            lambda attributes: self.flatten_dict_values(attributes))
        df['ruleId'] = df['rule'].apply(self.get_rule_id)
        df['isRandom'] = df['rule'].apply(lambda r: True if r.get('isRandomProbability') is True else False)
        df['rulePosition'] = df.apply(lambda d: self.get_rule_position(d['rule'], d['position']), axis=1)
        df['attributes'] = df.apply(lambda x: self.clean_attributes(x['userAttributes'], x['ruleOutputAttributes']),
                                    axis=1)
        if is_split_agree_not_agree:
            df.loc[df.status == 'NOT_AGREE', 'ruleId'] = 0 - df['ruleId'].astype(int)
        if is_add_vector:
            df['vector'] = df.apply(
                lambda x: self.vector_service.calculate_flow_attributes_vector(x['flow'], x['attributes']), axis=1)
            columns.append('vector')

        df = df[columns]
        df = self.validate_rules(df)
        return df
