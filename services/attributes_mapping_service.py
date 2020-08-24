import json
import os

from services.lambda_invoke_service import LambdaInvokeService

from utils.singelton import SingletonDecorator


@SingletonDecorator
class AttributesMappingService:
    def __init__(self, lambda_invoke_service=None):
        env = os.getenv('ENV', 'dev')

        calculated_attributes_name = f'attributes-mapping-{env}-get_calculated_attributes'
        self.calculated_attributes_lambda_name = os.getenv('CALCULATED_ATTRIBUTES_LAMBDA_NAME',
                                                           calculated_attributes_name)

        provide_attributes_lambda = f'attributes-mapping-{env}-provide_attributes_mapping'
        self.provide_attributes_lambda_name = os.getenv('PROVIDE_ATTRIBUTES_LAMBDA_NAME', provide_attributes_lambda)

        self.lambda_invoke_service = lambda_invoke_service or LambdaInvokeService()

    def get_calculated_attributes(self, user_attributes):
        response = self.lambda_invoke_service.invoke_lambda(self.calculated_attributes_lambda_name, user_attributes)
        calculated_attributes = json.loads(json.loads(response)['body'])
        return calculated_attributes

    def provide_attributes_mappings(self):
        response = self.lambda_invoke_service.invoke_lambda(self.provide_attributes_lambda_name, {})
        calculated_attributes = json.loads(json.loads(response)['body'])
        return calculated_attributes
