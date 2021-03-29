import json
import boto3
from time import sleep
from botocore.exceptions import ClientError
from create_environment.config_parser import CONFIG


def create_airflow_securitygroup(airflow_securitygroup, config):
    """
    Create necessary Security Group for AWS MWAA Airflow environment.
    
    Parameters:
    airflow_securitygroup (boto3 client): boto3 client for creation security group on AWS
    config (CONFIG): configuration file variable objects
    """
    try:
        security_group_id = None
        response = airflow_securitygroup.create_security_group(Description='airflow usage',
                                                          GroupName=config["AWS_AIR"]["SECURITY_GROUP_NAME"],
                                                          VpcId=config["AWS_AIR"]["VPC_ID"])
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            security_group_id = response["GroupId"]
            print("Created Airflow Subgroup")
        else:
            print("Failed create Airflow Subgroup.")
    except ClientError as e:
        print(e)
        if e.response["Error"]["Code"] == 'InvalidGroup.Duplicate':
            security_group_id = airflow_securitygroup.describe_security_groups(GroupNames=[config["AWS_AIR"]["SECURITY_GROUP_NAME"]])["SecurityGroups"][0]["GroupId"]
    return security_group_id

def create_redshift_cluster(redshift, config):
    """
    Create Redshift Cluster on AWS for staging, fact and dimensions tables.
    
    Parameters:
    redshift (boto3 client): boto3 client for creation Redshift Cluster on AWS
    config (CONFIG): configuration file variable objects
    """
    try:
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
    print("Waiting for cluster {} to be created...".format(cluster_identifier))
    is_created = False
    while not is_created:
        sleep(1)
        props = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)["Clusters"][0]
        is_created = props["ClusterStatus"] == "available"
    print("Cluster {} created.".format(cluster_identifier))


def create_airflow(airflow, config, security_group_id):
    """
    Create MWAA Airflow Environment on AWS for ETL job automation and monitoring
    
    Parameters:
    airflow (boto3 client): boto3 client for creation Redshift Cluster on AWS
    config (CONFIG): configuration file variable objects
    security_group_id (string): security group id for MWAA Airflow Environment network
    """
    try:
        response = airflow.create_environment(
            AirflowVersion=config["AWS_AIR"]["VERSION"],
            EnvironmentClass="mw1.medium",
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
            MaxWorkers=10,
            Name=config["AWS_AIR"]["ENVIRONMENT_NAME"],
            ExecutionRoleArn=config["AWS_AIR"]["EXECUTION_ROLE_ARN"],
            NetworkConfiguration={
                'SecurityGroupIds': [security_group_id],
                'SubnetIds': config["AWS_AIR"]["SUBNET_PRIVATE_IDS"].split(",")
            },
            SourceBucketArn=config["AWS_AIR"]["SOURCE_BUCKET_ARN"],
            WebserverAccessMode='PRIVATE_ONLY'
        )
    except ClientError as e:
        print(e)
    environment_name = config["AWS_AIR"]["ENVIRONMENT_NAME"]
    print("Waiting for Airflow environment {} to be created...".format(environment_name))
    is_created = False
    while not is_created:
        sleep(1)
        status_response = airflow.get_environment(Name=environment_name)["Environment"]
        is_created = status_response["Status"] == "AVAILABLE"
    print("Airflow environment {} created.".format(environment_name))

redshift = boto3.client(
        'redshift',
        region_name=CONFIG["AWS_DWH"]["REGION"],
        aws_access_key_id=CONFIG["AWS_ACCESS"]["KEY"],
        aws_secret_access_key=CONFIG["AWS_ACCESS"]["SECRET"]
    )

airflow = boto3.client(
        'mwaa',
        region_name=CONFIG["AWS_AIR"]["REGION"],
        aws_access_key_id=CONFIG["AWS_ACCESS"]["KEY"],
        aws_secret_access_key=CONFIG["AWS_ACCESS"]["SECRET"]
    )

airflow_securitygroup = boto3.client(
        'ec2',
        region_name=CONFIG["AWS_AIR"]["REGION"],
        aws_access_key_id=CONFIG["AWS_ACCESS"]["KEY"],
        aws_secret_access_key=CONFIG["AWS_ACCESS"]["SECRET"]
    )

if CONFIG["AWS_ENV_CREATE"]["REDSHIFT"]:
    create_redshift_cluster(redshift, CONFIG)

if CONFIG["AWS_ENV_CREATE"]["AIRFLOW"]:
    security_group_id = create_airflow_securitygroup(airflow_securitygroup, CONFIG)
    if security_group_id is not None:
        create_airflow(airflow, CONFIG, security_group_id)
    else:
        create_airflow(airflow, CONFIG, CONFIG["AWS_AIR"]["SECURITY_GROUP_ID"])
