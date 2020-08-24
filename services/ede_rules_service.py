import os
import json
from utils.logger import get_logger
from services.lambda_invoke_service import LambdaInvokeService

from utils.singelton import SingletonDecorator


@SingletonDecorator
class EdeRulesService:
    def __init__(self, lambda_invoke_service=None):
        self.logger = get_logger('EdeRulesService')
        env = os.getenv('ENV', 'dev')

        retrieve_rules_name = f'ede-rules-{env}-retrieve_rules'
        self.retrieve_rules_lambda_name = os.getenv('RULES_ACTION_LAMBDA_NAME', retrieve_rules_name)

        self.lambda_invoke_service = lambda_invoke_service or LambdaInvokeService()

    def get_rules(self, products_types: list):
        self.logger.info(f'get rules for {products_types}')
        event = {'productsTypes': products_types, 'onlyIds': True}
        response = self.lambda_invoke_service.invoke_lambda(self.retrieve_rules_lambda_name, event)
        response_body = json.loads(json.loads(response)['body'])
        return response_body
