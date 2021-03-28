from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageCSVToRedshiftOperator(BaseOperator):
    """
    This operator load data from S3 csv files to AWS Redshift staging table.

    Parameters:
    target_table (string): target staging table name
    columns (string): target table columns specific order.
    source_s3_path (string): link of data source s3 bucket
    aws_credential_conn_id (string): conn id of defined AWS credential details on Airflow
    redshift_conn_id (string): conn id of defined Redshift connection details on Airflow
    aws_region (string): AWS region of source data
    manifest (string): If FROM given S3 manifest file this parameter must be 'manifest'. All same type files names can collected in one manifest file. It can load multi file in one request.
    ignore_header (string): if this parameter specified as 'IGNOREHEADER 1', function except first line as header line of document
    """
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 target_table=None,
                 columns=None,
                 source_s3_path=None,
                 aws_credential_conn_id="aws_credential",
                 redshift_conn_id="redshift",
                 aws_region="us-east-1",
                 manifest="",
                 ignore_header="",
                 *args, **kwargs):
        super(StageCSVToRedshiftOperator, self).__init__(*args, **kwargs)
        self.target_table = target_table
        self.columns = columns
        self.source_s3_path = source_s3_path
        self.aws_credential_conn_id = aws_credential_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_region = aws_region
        self.ignore_header= ignore_header
        self.copy_template = """COPY {} ( {} )
                                FROM '{}'
                                ACCESS_KEY_ID '{}'
                                SECRET_ACCESS_KEY '{}'
                                REGION AS '{}'
                                FORMAT CSV
                                {}
                                ACCEPTINVCHARS 'e'
                             """ + " " + manifest

    def execute(self, context):
        self.log.info('Credentials loading..')
        aws_hook = AwsHook(self.aws_credential_conn_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)

        self.log.info("Stage table {} clear before load data..".format(self.target_table))
        redshift_hook.run("DELETE FROM {}".format(self.target_table))

        self.log.info("Creating load script for {}..".format(self.target_table))
        copy_script = self.copy_template.format(self.target_table,
                                                self.columns,
                                                self.source_s3_path,
                                                credentials.access_key,
                                                credentials.secret_key,
                                                self.aws_region,
                                                self.ignore_header)

        self.log.info("Load data to stage table {}..".format(self.target_table))
        redshift_hook.run(copy_script)







