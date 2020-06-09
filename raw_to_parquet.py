from airflow.models import DAG
import os
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
# from airflow.operators.s3_file_transform_operator import S3FileTransformOperator
from datetime import datetime
from tempfile import NamedTemporaryFile
import subprocess
import sys

from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3FileTransformOperator(BaseOperator):

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
        super(S3FileTransformOperator, self).__init__(*args, **kwargs)
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
        if self.transform_script is None and self.select_expression is None:
            raise AirflowException(
                "Either transform_script or select_expression must be specified")

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

            if self.select_expression is not None:
                content = source_s3.select_key(
                    key=self.source_s3_key,
                    expression=self.select_expression
                )
                f_source.write(content.encode("utf-8"))
            else:
                source_s3_key_object.download_fileobj(Fileobj=f_source)
            f_source.flush()

            if self.transform_script is not None:
                dir = os.path.dirname(os.path.abspath(__file__))
                result = subprocess.run(["ls", "-l", dir],
                                        universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                print(result.stdout)
                print(result.stderr)
                subprocess.Popen(["chown", "airflow:airflow", f"{dir}/utils"])
                # result = subprocess.Popen(["who"], universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                # print(result.stdout)
                # print(result.stderr)
                result = subprocess.Popen(["chmod", "0755 ", f"{dir}/{self.transform_script}"],
                                 universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                print(result.stdout)
                print(result.stderr)
                result = subprocess.run(["ls", "-l", dir], universal_newlines=True, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)
                print(result.stdout)
                print(result.stderr)
                process = subprocess.Popen(
                    [f'{dir}/{self.transform_script}', f_source.name, f_dest.name],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT
                )

                self.log.info("Output:")
                for line in iter(process.stdout.readline, b''):
                    self.log.info(line.decode(self.output_encoding).rstrip())

                process.wait()

                if process.returncode > 0:
                    raise AirflowException(
                        "Transform script failed: {0}".format(process.returncode)
                    )
                else:
                    self.log.info(
                        "Transform script successful. Output temporarily located at %s",
                        f_dest.name
                    )

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


Dag = DAG(
    dag_id='athena_query_and_move',
    schedule_interval=None,
    start_date=datetime(2019, 6, 7)
)

bucket_name = "scalez-airflow"
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
        and from_iso8601_timestamp(timestamp) >= from_iso8601_timestamp('{{ execution_date - macros.timedelta(hours=1) }}')
        and from_iso8601_timestamp(timestamp) <= from_iso8601_timestamp('{{ execution_date }}')
"""
with Dag as dag:
    run_query = XComEnabledAWSAthenaOperator(
        task_id='run_query',
        query=query,
        output_location='s3://scalez-airflow/csv-sessions/',
        database='my_database'
    )

    move_results = S3FileTransformOperator(
        task_id='move_results',
        source_s3_key='s3://scalez-airflow/csv-sessions/{{ task_instance.xcom_pull(task_ids="run_query") }}.csv',
        dest_s3_key='s3://scalez-airflow/sessions/date={{ execution_date - macros.timedelta(hours=1) }}/{{task_instance_key_str}}.parquet',
        transform_script='utils/csv_to_parquet.py'
    )

    # build_table = ""
    # update_partitions = ""

move_results.set_upstream(run_query)




