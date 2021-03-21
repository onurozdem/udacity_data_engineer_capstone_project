import boto3
from time import sleep
from create_environment.config_parser import CONFIG
from botocore.exceptions import ClientError


def create_redshift_cluster(redshift, config):
    try:
        #sg-00f5844e1382d74b7
        response = redshift.create_cluster(
            ClusterType=config["AWS_DWH"]["CLUSTER_TYPE"],
            NodeType=config["AWS_DWH"]["NODE_TYPE"],
            NumberOfNodes=int(config["AWS_DWH"]["NUM_NODES"]),
            DBName=config["AWS_DWH"]["DB_NAME"],
            ClusterIdentifier=config["AWS_DWH"]["CLUSTER_IDENTIFIER"],
            MasterUsername=config["AWS_DWH"]["DB_USER"],
            MasterUserPassword=config["AWS_DWH"]["DB_PASSWORD"],
            IamRoles=[config["IAM_ROLE"]["REDSHIFT_ARN"]],
            EnhancedVpcRouting=True
        )
    except ClientError as e:
        print(e)
    cluster_identifier = config["AWS_DWH"]["CLUSTER_IDENTIFIER"]
    props = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)["Clusters"][0]
    print("Waiting for cluster {} to be created...".format(cluster_identifier))
    is_created = False
    while not is_created:
        sleep(1)
        props = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)["Clusters"][0]
        is_created = props["ClusterStatus"] == "available"
    print("Cluster {} created.".format(cluster_identifier))
    return props

def create_airflow_s3(airflow_s3, config):
    try:
        response = airflow_s3.create_bucket(
            ACL='private',
            Bucket=config["AWS_AIR"]["BUCKET"])

        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print("Created Airflow S3 bucket on: s3://{}".format(response["Location"]))

            response_dag = airflow_s3.put_object(Bucket=config["AWS_AIR"]["BUCKET"], Key=(config["AWS_AIR"]["DAG_PATH"]+'/'))
            if response_dag["ResponseMetadata"]["HTTPStatusCode"] == 200:
                print("Created dag path on: s3://{}".format(response["Location"]))
            else:
                print("Failed create_environment dag path.")

            response_plugin = airflow_s3.put_object(Bucket=config["AWS_AIR"]["BUCKET"], Key=(config["AWS_AIR"]["PLUGIN_PATH"] + '/'))
            if response_plugin["ResponseMetadata"]["HTTPStatusCode"] == 200:
                print("Created plugin path on: s3://{}".format(response["Location"]))
            else:
                print("Failed create_environment plugin path")
        else:
            print("Failed create_environment airflow bucket.")
    except ClientError as e:
        print(e)

def create_airflow(airflow, config):
    # TODO: create_environment exeute role and vpc
    try:
        response = airflow.create_environment(
            AirflowVersion='1.10.13',
            EnvironmentClass="",
            DagS3Path=config["AWS_AIR"]["DAG_PATH"],
            LoggingConfiguration={
                'DagProcessingLogs': {
                    'Enabled': False,
                    'LogLevel': 'INFO'
                },
                'SchedulerLogs': {
                    'Enabled': False,
                    'LogLevel': 'INFO'
                },
                'TaskLogs': {
                    'Enabled': True,
                    'LogLevel': 'INFO'
                },
                'WebserverLogs': {
                    'Enabled': False,
                    'LogLevel': 'INFO'
                },
                'WorkerLogs': {
                    'Enabled': False,
                    'LogLevel': 'INFO'
                }
            },
            MaxWorkers=123,
            MinWorkers=2,
            Name=config["AWS_AIR"]["ENVIRONMENT_NAME"],
            ExecutionRoleArn=config[""][""],
            NetworkConfiguration="",
            SourceBucketArn=config["AWS_AIR"]["SOURCE_BUCKET_ARN"],
            WebserverAccessMode='PRIVATE_ONLY'
        )
    except ClientError as e:
        print(e)
    #props = airflow.get_environment(Name=config["AWS_AIR"]["ENVIRONMENT_NAME"])
    #print("Waiting for  {} to be created...".format(props))

redshift = boto3.client(
        'redshift',
        region_name=CONFIG["AWS_DWH"]["REGION"],
        aws_access_key_id=CONFIG["AWS_ACCESS"]["KEY"],
        aws_secret_access_key=CONFIG["AWS_ACCESS"]["SECRET"]
    )

airflow_s3 = boto3.client(
        's3',
        region_name=CONFIG["AWS_AIR"]["S3_REGION"],
        aws_access_key_id=CONFIG["AWS_ACCESS"]["KEY"],
        aws_secret_access_key=CONFIG["AWS_ACCESS"]["SECRET"]
    )

airflow = boto3.client(
        'mwaa',
        region_name=CONFIG["AWS_AIR"]["REGION"],
        aws_access_key_id=CONFIG["AWS_ACCESS"]["KEY"],
        aws_secret_access_key=CONFIG["AWS_ACCESS"]["SECRET"]
    )

create_redshift_cluster(redshift, CONFIG)
create_airflow_s3(airflow_s3, CONFIG)
#create_airflow(airflow, CONFIG)
