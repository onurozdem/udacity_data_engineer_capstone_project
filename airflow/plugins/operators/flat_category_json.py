import json
import boto3
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class FlatCategoryJSONOperator(BaseOperator):
    """

    """
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 source_s3_bucket=None,
                 source_s3_key=None,
                 s3_credential_conn_id="aws_credential",
                 *args, **kwargs):
        super(FlatCategoryJSONOperator, self).__init__(*args, **kwargs)
        self.source_s3_bucket = source_s3_bucket
        self.source_s3_key = source_s3_key
        self.s3_credential_conn_id = s3_credential_conn_id

    def execute(self, context):
        self.log.info('Credentials loading..')
        aws_hook = AwsHook(self.s3_credential_conn_id)
        credentials = aws_hook.get_credentials()

        s3 = boto3.resource('s3',
                            aws_access_key_id=credentials.access_key,
                            aws_secret_access_key=credentials.secret_key)

        obj = s3.Object(self.source_s3_bucket, self.source_s3_key)
        body = obj.get()['Body'].read()

        content = json.loads(body)
        it = content["items"]
        str_body = ""
        for i in range(len(it)):
            it[i].update(it[i]["snippet"])
            del it[i]["snippet"]
            str_body = str_body + json.dumps(it[i]) + "\n"

        flatten_json_key = self.source_s3_key.replace(".json", "_flatten.json")
        obj = s3.Object(self.source_s3_bucket, flatten_json_key)

        obj.put(Body=str_body)
        self.log.info('Completed json flat process.')







