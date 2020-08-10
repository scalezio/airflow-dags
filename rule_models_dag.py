import json
from datetime import timedelta, datetime
from io import StringIO

import boto3
import pandas as pd
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from botocore.client import Config

from services.dynamo_db_service import DynamoDBService

default_args = {
    'owner': 'scalez',
    'depends_on_past': False,
    'start_date': datetime.strptime('2020-07-20', '%Y-%m-%d'),
    'email': ['daniel@scalez.io'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=30)
}

days_interval_train = 30

dag = DAG('rule_models', schedule_interval=timedelta(days=1), catchup=True,
          default_args=default_args)


def invoke_lambda(lambda_name, from_date, to_date, event_name):
    config_dict = {"connect_timeout": 5, "read_timeout": 900}
    config = Config(**config_dict)

    lambda_hook = AwsLambdaHook(function_name=lambda_name, region_name='us-east-1', config=config)
    payload = json.dumps({"eventName": event_name, "fromDate": from_date, "toDate": to_date})
    lambda_hook.invoke_lambda(payload)


def download_events_function(**context):
    to_date = context['ds']
    from_date = (datetime.strptime(to_date, '%Y-%m-%d') - timedelta(days=days_interval_train)).strftime('%Y-%m-%d')

    events_db_service = DynamoDBService('connectors-prod-events_table')
    items = events_db_service.get_query_items(p_key_name='eventName', p_key_value="UserRatedRule",
                                              s_key_name='timestamp', s_key_value_max=to_date,
                                              s_key_value_lower=from_date)

    if items:
        file_name = f'UserRatedRule/raw_events_{from_date}_{to_date}.csv'
        csv_buffer = StringIO()
        pd.DataFrame(items).to_csv(csv_buffer)
        bucket_name = f'rules-models-prod-data-369120691906'
        boto3.resource('s3').Object(bucket_name, file_name).put(Body=csv_buffer.getvalue())


def invoke_download_events(**context):
    to_date = context['ds']
    from_date = (datetime.strptime(to_date, '%Y-%m-%d') - timedelta(days=days_interval_train)).strftime('%Y-%m-%d')
    print(f'invoke_download_events to_date: {to_date}, from_date: {from_date}')
    invoke_lambda(lambda_name="rules-models-prod-download_events", from_date=from_date, to_date=to_date,
                  event_name="UserRatedRule")


def invoke_train_rule_probability_model(**context):
    to_date = context['ds']
    from_date = (datetime.strptime(to_date, '%Y-%m-%d') - timedelta(days=days_interval_train)).strftime('%Y-%m-%d')
    invoke_lambda(lambda_name="rules-models-prod-train_rule_probability_model", from_date=from_date, to_date=to_date,
                  event_name="UserRatedRule")


def invoke_train_rule_combinations_model(**context):
    to_date = context['ds']
    from_date = (datetime.strptime(to_date, '%Y-%m-%d') - timedelta(days=days_interval_train)).strftime('%Y-%m-%d')
    invoke_lambda(lambda_name="rules-models-prod-train_rule_combinations_model", from_date=from_date, to_date=to_date,
                  event_name="UserRatedRule")


download_events_operator = PythonOperator(task_id='download_events_operator',
                                          provide_context=True,
                                          python_callable=download_events_function,
                                          dag=dag)

train_rule_probability_model_operator = PythonOperator(task_id='train_rule_probability_model_operator',
                                                       provide_context=True,
                                                       python_callable=invoke_train_rule_probability_model,
                                                       dag=dag)

train_rule_combinations_model_operator = PythonOperator(task_id='train_rule_combinations_model_operator',
                                                        provide_context=True,
                                                        python_callable=invoke_train_rule_combinations_model,
                                                        dag=dag)

download_events_operator >> [train_rule_probability_model_operator, train_rule_combinations_model_operator]
