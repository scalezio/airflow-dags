import datetime as dt

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


def greet():
    print('Writing in file')
    return 'Greeted'


def respond():
    return 'Greet Responded Again'


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'concurrency': 1,
    'retries': 5
}

with DAG('my_simple_dag',
         catchup=False,
         default_args=default_args,
         schedule_interval='*/10 * * * *',
         # schedule_interval=None,
         ) as dag:

    opr_greet = PythonOperator(task_id='greet',
                               python_callable=greet)

    opr_respond = PythonOperator(task_id='respond',
                                 python_callable=respond)

opr_greet >> opr_respond
