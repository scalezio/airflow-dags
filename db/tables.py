import os

env = os.getenv('ENV', 'prod')


class RulesModelsTable:
    TABLE_NAME = os.getenv('EDE_RULES_MODELS_TABLE', f'ede-rules-{env}-rule_models_table')
    MODEL_NAME = 'modelName'
    MODEL = 'model'
    RULES_PROBABILITY_MODEL = 'rulesProbabilityModel'
    RULES_COMBINATIONS_MODEL = 'rulesCombinationsModel'


class HierarchyTable:
    TABLE_NAME = os.getenv('TASKS_PARAMS_TABLE', f'cs-experience-{env}-hierarchy')
    HIERARCHY = "hierarchy"
    HIERARCHY_DICT = "hierarchy_dict"
    RANDOM_PERCENT = "randomPercent"
    VERSION = "version"
    PARAMETERS = "parameters"


class EventsTable:
    TABLE_NAME = os.getenv('EVENTS_TABLE', f'connectors-{env}-events_table')
    EVENT_NAME = 'eventName'
    TIMESTAMP = 'timestamp'
    PAYLOAD = 'payload'
