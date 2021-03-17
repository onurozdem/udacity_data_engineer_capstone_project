from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageJSONToRedshiftOperator(BaseOperator):
    """
    This operator load data from S3 json files to AWS Redshift staging table. 
    
    Parameters:
    target_table (string): target staging table name
    source_s3_path (string): link of data source s3 bucket
    aws_credential_conn_id (string): conn id of defined AWS credential details on Airflow
    redshift_conn_id (string): conn id of defined Redshift connection details on Airflow
    aws_region (string): AWS region of source data
    json_option (string): mapping for between source and target columns. you can assign 'auto' or JSON path file
    """
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 target_table=None,
                 source_s3_path=None,
                 aws_credential_conn_id="aws_credential",
                 redshift_conn_id="redshift",
                 aws_region="us-east-1",
                 json_option="auto ignorecase",
                 manifest="",
                 *args, **kwargs):

        super(StageJSONToRedshiftOperator, self).__init__(*args, **kwargs)
        self.target_table = target_table
        self.source_s3_path = source_s3_path
        self.aws_credential_conn_id = aws_credential_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_region = aws_region
        self.json_option = json_option
        if manifest != "":
            self.copy_template = """COPY {}
                                    FROM '{}'
                                    ACCESS_KEY_ID '{}'
                                    SECRET_ACCESS_KEY '{}'
                                    REGION AS '{}'
                                 """ + " " + manifest
        elif manifest == "manifest":
            self.copy_template = """COPY {}
                                    FROM '{}'
                                    ACCESS_KEY_ID '{}'
                                    SECRET_ACCESS_KEY '{}'
                                    REGION AS '{}'
                                    FORMAT as json '{}'
                                 """

    def execute(self, context):
        self.log.info('Credentials loading..')
        aws_hook = AwsHook(self.aws_credential_conn_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        self.log.info("Stage table {} clear before load data..".format(self.target_table))
        redshift_hook.run("DELETE FROM {}".format(self.target_table))
        
        self.log.info("Creating load script for {}..".format(self.target_table))
        copy_script = self.copy_template.format(self.target_table,
                                                self.source_s3_path,
                                                credentials.access_key,
                                                credentials.secret_key,
                                                self.aws_region,
                                                self.json_option)
        
        self.log.info("Load data to stage table {}..".format(self.target_table))
        redshift_hook.run(copy_script)
        
        





