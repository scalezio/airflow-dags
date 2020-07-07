import json
from datetime import timedelta, datetime
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook
from airflow.models import DAG
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'scalez',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['daniel@scalez.io'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

dag = DAG('rule_probability_model', schedule_interval=timedelta(days=1), catchup=True, default_args=default_args)


def invoke_download_events(**context):
    to_date = context['ds']
    from_date = context['prev_ds'] or (datetime.strptime(to_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

    download_events_hook = AwsLambdaHook(function_name='rules-models-prod-download_events', region_name='us-east-1')
    payload = json.dumps({"eventName": "UserRatedRule", "fromDate": from_date, "toDate": to_date})
    download_events_hook.invoke_lambda(payload)


def invoke_train_rule_probability_model(**context):
    to_date = context['ds']
    from_date = (datetime.strptime(to_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')

    train_rule_probability_model_hook = AwsLambdaHook(function_name='rules-models-prod-train_rule_probability_model',
                                                      region_name='us-east-1')
    payload = json.dumps({"eventName": "UserRatedRule", "fromDate": from_date, "toDate": to_date})
    train_rule_probability_model_hook.invoke_lambda(payload)


download_events_operator = PythonOperator(task_id='download_events_operator',
                                          provide_context=True,
                                          python_callable=invoke_download_events,
                                          dag=dag)

train_rule_probability_model_operator = PythonOperator(task_id='train_rule_probability_model_operator',
                                                       provide_context=True,
                                                       python_callable=invoke_train_rule_probability_model,
                                                       dag=dag)

download_events_operator >> train_rule_probability_model_operator
