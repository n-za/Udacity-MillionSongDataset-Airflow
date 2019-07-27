from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import datetime
import logging


class StageToRedshiftOperator(BaseOperator):
    # Implementation is based on the code provided for Lesson 3 Exercice 1
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
        COMPUPDATE OFF
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 format_json="",
                 *args, **kwargs):
        
        """
            Constructor:        
            
            * redshift_conn_id a connection to redshift (It would be a bad idea to reconnect each time.)
            * aws_credentials: aws credentials in order to access a public S3 bucket and write into Amazon Redshift
            * s3_bucket: a public bucket where the raw data are stored
            * s3_key: prefix for the data in s3_bucket
            * json_format: list of fields to keep
            * generic arguments and keyword arguments
            
        """
    
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.format_json = format_json


    def execute(self, context):
        """
            Processor: It connects to S3, resets the staging table given in the field 'table', and inserts the S3 data into this table.
            
        """
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table: {}".format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))

        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.format_json
        )
        self.log.info("Copying data from S3 to Redshift: {}".format(formatted_sql))

        redshift.run(formatted_sql)
