from airflow.models import DAG
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
import pandas as pd
import numpy as np
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
            df = pd.read_csv(f_source.name).astype({
                'user_id': str,
                'ad_id': str
            })
            df.loc[(df.rule_id == 'nan'), 'ad_id'] = ''
            df.loc[(df.user_id == 'nan'), 'user_id'] = ''
            df.loc[(df.attribute_name == np.NaN), 'attribute_name'] = ''
            df.loc[(df.attribute_value == np.NaN), 'attribute_value'] = ''
            df.loc[(df.user_value == np.NaN), 'user_value'] = ''
            df.loc[(df.ref_url == np.NaN), 'ref_url'] = ''
            df.loc[(df.utm_source == np.NaN), 'utm_source'] = ''
            df.loc[(df.user_value == np.NaN), 'user_value'] = ''
            df.to_parquet(f_dest.name)

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
    'depends_on_past': True,
    'start_date': datetime(2020, 6, 1),
    'email': ['daniel@scalez.io'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

Dag = DAG('users_table', schedule_interval='0 * * * *', catchup=True, default_args=default_args)

bucket_name = "scalez-airflow"
query = """
    SELECT
        from_iso8601_timestamp(timestamp) timestamp,
        CAST("json_extract"("payload", '$.userid') AS varchar) "user_id",
        CAST("json_extract"("payload", '$.attributename') AS varchar) "attribute_name",
        CAST("json_extract"("payload", '$.attributevalue') AS varchar) "attribute_value",
        CAST("json_extract"("payload", '$.uservalue') AS varchar) "user_value",
        CAST("json_extract"("payload", '$.utmcampiang') AS varchar) "ad_id",
        CAST("json_extract"("payload", '$.utmrefurl') AS varchar) "ref_url",
        CAST("json_extract"("payload", '$.utmsource') AS varchar) "utm_source",
        (case when event='NewSubscriber' then 1 else 0 end) is_new_subscriber
    FROM
        internal.scalez_events
    WHERE 
        "event" = 'NewUserAttribute' OR 
        "event" = 'UserClickedReferral' OR 
        "event" = 'NewSubscriber' and 
        from_iso8601_timestamp(timestamp) >= from_iso8601_timestamp('{{ prev_execution_date }}')
        and from_iso8601_timestamp(timestamp) <= from_iso8601_timestamp('{{ execution_date }}')
"""
with Dag as dag:
    run_query = XComEnabledAWSAthenaOperator(
        task_id='run_query_users',
        query=query,
        output_location='s3://scalez-airflow/csv-users/',
        database='my_database'
    )

    move_results = S3CSVtoParquet(
        task_id='move_results_users',
        source_s3_key='s3://scalez-airflow/csv-users/{{ task_instance.xcom_pull(task_ids="run_query") }}.csv',
        dest_s3_key='s3://scalez-airflow/users/date={{ prev_ds }}/{{execution_date}}.parquet'
    )

    fix_partitions = XComEnabledAWSAthenaOperator(
        task_id='fix_partitions_users',
        query="MSCK REPAIR TABLE silver_tables.users;",
        output_location='s3://scalez-airflow/repair/',
        database='my_database'
    )

    # build_table = ""
    # update_partitions = ""

move_results >> run_query >> fix_partitions



