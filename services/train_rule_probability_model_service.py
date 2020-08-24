from db.tables import RulesModelsTable
from services.model_trainer_service import ModelTrainerService
from utils.binomal_utils import binom_conf_interval


class TrainRuleProbabilityModelService:
    def __init__(self):
        self.model_trainer_service = ModelTrainerService()

    @staticmethod
    def calculate_lower_conf_interval_probability(df):
        rules_agree_probability = {
            flow: {str(rule_id): {
                'ruleProbability':
                    binom_conf_interval(df_rule[df_rule['status'] == 'AGREE'].__len__(), df_rule.__len__(),
                                        0.95)[0]} for rule_id, df_rule in df_flow.groupby(['ruleId'])}
            for flow, df_flow in df.groupby(['flow'])}
        return rules_agree_probability

    def train_rule_model(self, from_date, to_date, df=None):
        self.model_trainer_service.train_rule_model(model_name=RulesModelsTable.RULES_PROBABILITY_MODEL,
                                                    train_model_func=self.calculate_lower_conf_interval_probability,
                                                    from_date=from_date, to_date=to_date, df=df)
