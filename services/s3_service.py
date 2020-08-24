import boto3
import json
from utils.singelton import SingletonDecorator

DELIMITER = '\n\n'
DELIMITER_ONE = '\n'


@SingletonDecorator
class S3Service:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')

    @staticmethod
    def get_content_list(str_obj):
        try:
            return [json.loads(content_str) for content_str in str_obj.split(DELIMITER) if content_str]
        except:
            return [json.loads(content_str) for content_str in str_obj.split(DELIMITER_ONE) if content_str]

    def get_s3_file_content(self, file) -> list:
        bucket_name = file['s3']['bucket']['name']
        key = file['s3']['object']['key']
        return self.download_s3_file(bucket_name, str(key))

    def download_s3_file(self, bucket_name: str, key: str) -> list:
        if not key.startswith('processing-failed'):
            body = self.s3_resource.Object(bucket_name, key).get()['Body'].read()
            content_str = body.decode('utf8')
            content_list = self.get_content_list(content_str)
            return content_list
        return []

    def iterate_bucket_items(self, bucket_name, paginate_parameters=None):
        paginate_parameters = paginate_parameters or {}
        paginator = self.s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, **paginate_parameters)

        for page in page_iterator:
            if page['KeyCount'] > 0:
                for item in page['Contents']:
                    yield item
