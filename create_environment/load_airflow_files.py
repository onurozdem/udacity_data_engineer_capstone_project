import boto3
from create_environment.config_parser import CONFIG

airflow_s3 = boto3.client(
        's3',
        region_name=CONFIG["AWS_AIR"]["S3_REGION"],
        aws_access_key_id=CONFIG["AWS_ACCESS"]["KEY"],
        aws_secret_access_key=CONFIG["AWS_ACCESS"]["SECRET"]
    )

print("Load dags files..")
airflow_s3.upload_file("../airflow/dags/etl_dag.py", CONFIG["AWS_AIR"]["BUCKET"], "dags/etl_dag.py")
airflow_s3.upload_file("../airflow/dags/category_source_meta.json", CONFIG["AWS_AIR"]["BUCKET"], "dags/category_source_meta.json")

print("Load plugin zip..")
airflow_s3.upload_file("../airflow/plugins.zip", CONFIG["AWS_AIR"]["BUCKET"], "plugins.zip")

print("Load create table script..")
airflow_s3.upload_file("../airflow/create_tables.sql", CONFIG["AWS_AIR"]["BUCKET"], "create_tables.sql")