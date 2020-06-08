from airflow.models import DAG
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator
from datetime import datetime


class XComEnabledAWSAthenaOperator(AWSAthenaOperator):
    def execute(self, context):
        super(XComEnabledAWSAthenaOperator, self).execute(context)
        # just so that this gets `xcom_push`(ed)
        return self.query_execution_id


Dag = DAG(
    dag_id='athena_query_and_move',
    schedule_interval=None,
    start_date=datetime(2019, 6, 7)
)


query = """
    SELECT
    from_iso8601_timestamp(timestamp),
    CAST("json_extract"("payload", '$.userid') AS varchar) "user_id",
    CAST("json_extract"("payload", '$.sessionid') AS varchar) "session_id",
    CAST("json_extract"("payload", '$.taskname') AS varchar) "task_name",
    CAST("json_extract"("payload", '$.taskresultname') AS varchar) "task_result_name",
    CAST("json_extract"("payload", '$.taskid') AS varchar) "task_id"
    FROM
        internal.scalez_events
    WHERE
        event = 'UserPickedProductTypes'
        and from_iso8601_timestamp(timestamp) >= timestamp '{{ prev_execution_date }}' 
        and from_iso8601_timestamp(timestamp) <= timestamp '{{ execution_date }}' 
"""
with Dag as dag:
    run_query = XComEnabledAWSAthenaOperator(
        task_id='run_query',
        query=query,
        output_location='s3://airflow/{{ task_instance_key_str }}/',
        database='my_database'
    )

    # move_results = S3FileTransformOperator(
    #     task_id='move_results',
    #     source_s3_key='s3://airflow/{{ task_instance_key_str }}/{{ task_instance.xcom_pull(task_ids="run_query") }}.csv',
    #     dest_s3_key='s3://mybucket/otherpath/myresults.parquet',
    #     transform_script='csv_to_parquet.py'
    # )
run_query
# move_results.set_upstream(run_query)