from airflow.models import DAG
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
import pandas as pd
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile
import sys

from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3CSVtoParquet(BaseOperator):

    template_fields = ('source_s3_key', 'dest_s3_key')
    template_ext = ()
    ui_color = '#f9c915'

    @apply_defaults
    def __init__(
            self,
            source_s3_key,
            dest_s3_key,
            transform_script=None,
            select_expression=None,
            source_aws_conn_id='aws_default',
            source_verify=None,
            dest_aws_conn_id='aws_default',
            dest_verify=None,
            replace=False,
            *args, **kwargs):
        super(S3CSVtoParquet, self).__init__(*args, **kwargs)
        self.source_s3_key = source_s3_key
        self.source_aws_conn_id = source_aws_conn_id
        self.source_verify = source_verify
        self.dest_s3_key = dest_s3_key
        self.dest_aws_conn_id = dest_aws_conn_id
        self.dest_verify = dest_verify
        self.replace = replace
        self.transform_script = transform_script
        self.select_expression = select_expression
        self.output_encoding = sys.getdefaultencoding()

    def execute(self, context):

        source_s3 = S3Hook(aws_conn_id=self.source_aws_conn_id,
                           verify=self.source_verify)
        dest_s3 = S3Hook(aws_conn_id=self.dest_aws_conn_id,
                         verify=self.dest_verify)

        self.log.info("Downloading source S3 file %s", self.source_s3_key)
        if not source_s3.check_for_key(self.source_s3_key):
            raise AirflowException(
                "The source key {0} does not exist".format(self.source_s3_key))
        source_s3_key_object = source_s3.get_key(self.source_s3_key)

        with NamedTemporaryFile("wb") as f_source, NamedTemporaryFile("wb") as f_dest:
            self.log.info(
                "Dumping S3 file %s contents to local file %s",
                self.source_s3_key, f_source.name
            )
            source_s3_key_object.download_fileobj(Fileobj=f_source)
            f_source.flush()

            # transform to parquet
            df = pd.read_csv(f_source.name, parse_dates=['timestamp']).astype({
                'user_id': str
            })
            df.loc[(df.user_id == 'nan'), 'user_id'] = ''
            df.to_parquet(f_dest.name, coerce_timestamps='ms', allow_truncated_timestamps=True)

            self.log.info("Uploading transformed file to S3")
            f_dest.flush()
            dest_s3.load_file(
                filename=f_dest.name,
                key=self.dest_s3_key,
                replace=self.replace
            )
            self.log.info("Upload successful")


class XComEnabledAWSAthenaOperator(AWSAthenaOperator):
    def execute(self, context):
        super(XComEnabledAWSAthenaOperator, self).execute(context)
        # just so that this gets `xcom_push`(ed)
        return self.query_execution_id


default_args = {
    'owner': 'scalez',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 28),
    'email': ['daniel@scalez.io'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

Dag = DAG('tasks_table', schedule_interval='30 10 * * *', catchup=True, default_args=default_args)

bucket_name = "scalez-airflow"
query = """
    SELECT timestamp,
         event,
         CAST("json_extract"("payload",
         '$.taskname') AS varchar) "task_name", CAST("json_extract"("payload", '$.taskid') AS varchar) "task_id", 
         CAST("json_extract"("payload", '$.tasktype') AS varchar) "task_type", 
         CAST("json_extract"("payload", '$.taskdata.userid') AS varchar) "user_id", 
         CAST("json_extract"("payload", '$.taskdata.stylistid') AS varchar) "stylist_id",
         CASE WHEN CAST("json_extract"("payload", '$.taskdata.originalstylistid') AS varchar) is NOT NULL 
            THEN CAST("json_extract"("payload", '$.taskdata.originalstylistid') AS varchar)
            ELSE CAST("json_extract"("payload", '$.taskdata.stylistid') AS varchar)
            END "original_stylist_id", date
    FROM internal.scalez_events
    WHERE event IN 
            (SELECT DISTINCT eventname
            FROM internal.scalez_events
            WHERE eventname LIKE '%Task%'
                    AND date = date '{{ ds }}')
        AND date = date '{{ ds }}'
"""
with Dag as dag:
    run_query = XComEnabledAWSAthenaOperator(
        task_id='run_query',
        query=query,
        output_location='s3://scalez-airflow/csv-tasks/',
        database='internal'
    )

    move_results = S3CSVtoParquet(
        task_id='move_results',
        source_s3_key='s3://scalez-airflow/csv-tasks/{{ task_instance.xcom_pull(task_ids="run_query") }}.csv',
        dest_s3_key='s3://scalez-airflow/tasks/date={{ ds }}/{{execution_date}}.parquet'
    )

    fix_partitions = XComEnabledAWSAthenaOperator(
        task_id='fix_partitions',
        query="ALTER TABLE silver_tables.tasks ADD IF NOT EXISTS PARTITION (date='{{ ds }}') LOCATION 's3://scalez-airflow/tasks/date={{ ds }}/';",
        output_location='s3://scalez-airflow/repair/',
        database='silver_tables'
    )

    # build_table = ""
    # update_partitions = ""

run_query >> move_results >> fix_partitions






