import boto3
import json
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
                'SecurityGroupIds': [config["AWS_AIR"]["SECURITY_GROUP_ID"]],
                'SubnetIds': config["AWS_AIR"]["SUBNET_PRIVATE_IDS"].split(",")
            },
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

airflow_iam = boto3.client(
        'iam',
        region_name=CONFIG["AWS_AIR"]["REGION"],
        aws_access_key_id=CONFIG["AWS_ACCESS"]["KEY"],
        aws_secret_access_key=CONFIG["AWS_ACCESS"]["SECRET"]
)



airflow_vpc = boto3.client('cloudformation',
                           region_name=CONFIG["AWS_AIR"]["REGION"],
                           aws_access_key_id=CONFIG["AWS_ACCESS"]["KEY"],
                           aws_secret_access_key=CONFIG["AWS_ACCESS"]["SECRET"]
                           )


airflow = boto3.client(
        'mwaa',
        region_name=CONFIG["AWS_AIR"]["REGION"],
        aws_access_key_id=CONFIG["AWS_ACCESS"]["KEY"],
        aws_secret_access_key=CONFIG["AWS_ACCESS"]["SECRET"]
    )

airflow_subgroup = boto3.client(
        'ec2',
        region_name=CONFIG["AWS_AIR"]["REGION"],
        aws_access_key_id=CONFIG["AWS_ACCESS"]["KEY"],
        aws_secret_access_key=CONFIG["AWS_ACCESS"]["SECRET"]
    )

"""create_redshift_cluster(redshift, CONFIG)
create_airflow_s3(airflow_s3, CONFIG)"""
create_airflow(airflow, CONFIG)



"""response = airflow_iam.create_policy(
    PolicyName='string',
    Path='string',
    PolicyDocument='string',
    Description='string',
    Tags=[
        {
            'Key': 'string',
            'Value': 'string'
        },
    ]
)"""

policy_document_content = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "airflow:PublishMetrics",
            "Resource": "arn:aws:airflow:us-east-1:638252266084:environment/uda-youtube-airflow-env"
        },
        {
            "Effect": "Deny",
            "Action": "s3:ListAllMyBuckets",
            "Resource": [
                "arn:aws:s3:::uda-airflow-bucket",
                "arn:aws:s3:::uda-airflow-bucket/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject*",
                "s3:GetBucket*",
                "s3:List*"
            ],
            "Resource": [
                "arn:aws:s3:::uda-airflow-bucket",
                "arn:aws:s3:::uda-airflow-bucket/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogStream",
                "logs:CreateLogGroup",
                "logs:PutLogEvents",
                "logs:GetLogEvents",
                "logs:GetLogRecord",
                "logs:GetLogGroupFields",
                "logs:GetQueryResults"
            ],
            "Resource": [
                "arn:aws:logs:us-east-1:638252266084:log-group:airflow-uda-youtube-airflow-env-*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:DescribeLogGroups"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": "cloudwatch:PutMetricData",
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "sqs:ChangeMessageVisibility",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes",
                "sqs:GetQueueUrl",
                "sqs:ReceiveMessage",
                "sqs:SendMessage"
            ],
            "Resource": "arn:aws:sqs:us-east-1:*:airflow-celery-*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "kms:Decrypt",
                "kms:DescribeKey",
                "kms:GenerateDataKey*",
                "kms:Encrypt"
            ],
            "NotResource": "arn:aws:kms:*:638252266084:key/*",
            "Condition": {
                "StringLike": {
                    "kms:ViaService": [
                        "sqs.us-east-1.amazonaws.com",
                        "airflow-env.amazonaws.com"
                    ]
                }
            }
        }
    ]
}


"""response = airflow_iam.create_policy(
    PolicyName=CONFIG["AWS_AIR_PRE"]["POLICY_NAME"],
    PolicyDocument=json.dumps(policy_document_content)
)"""

"""response1 = airflow_iam.create_role(
    RoleName=CONFIG["AWS_AIR_PRE"]["ROLE_NAME"],
    AssumeRolePolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": ["airflow-env.amazonaws.com","ec2.amazonaws.com"]},       "Action": "sts:AssumeRole"}]})
    )"""

"""response = airflow_iam.attach_role_policy(
    RoleName=CONFIG["AWS_AIR_PRE"]["ROLE_NAME"], PolicyArn='arn:aws:iam::638252266084:policy/uda_mwaa_policy')"""



"""response3 = airflow_vpc.create_stack(
    StackName=CONFIG["AWS_AIR_PRE"]["VPC_STACK_NAME"],
    TemplateURL=CONFIG["AWS_AIR_PRE"]["VPC_STACK_TEMPLATE"],
    OnFailure='DELETE'
)"""


"""response4 = airflow_subgroup.create_security_group(
    Description='airflow usage',
    GroupName=CONFIG["AWS_AIR"]["SECURITY_GROUP_NAME"],
    VpcId=CONFIG["AWS_AIR"]["VPC_ID"]
)"""