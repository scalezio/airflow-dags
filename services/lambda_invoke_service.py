import json
import os

import boto3
from utils.logger import get_logger


class LambdaInvokeService:
    def __init__(self):
        self.logger = get_logger('LambdaInvokeService')
        self.client = boto3.client('lambda', os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))

    def invoke_lambda(self, lambda_name: str, body: dict):
        response = self.client.invoke(
            FunctionName=lambda_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(body)
        )
        return response['Payload'].read().decode()
