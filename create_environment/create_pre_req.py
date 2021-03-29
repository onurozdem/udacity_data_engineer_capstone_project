import json
import boto3
from time import sleep
from botocore.exceptions import ClientError
from create_environment.config_parser import CONFIG


def create_airflow_policy(airflow_iam, config):
    """
    Create necessary Policy for AWS MWAA Airflow environment.
    
    Parameters:
    airflow_iam (boto3 client): boto3 client for creation policy on AWS IAM
    config (CONFIG): configuration file variable objects
    """
    try:
        policy_arn = None
        f = open("airflow_policy.json")
        policy_document_content = json.loads(f.read())
        f.close()

        response = airflow_iam.create_policy(PolicyName=config["AWS_AIR_PRE"]["POLICY_NAME"],
                                             PolicyDocument=json.dumps(policy_document_content))
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print("Created Airflow Policy: {}".format(response["Policy"]["Arn"]))
            policy_arn = response["Policy"]["Arn"]
        else:
            print("Failed create Airflow Policy.")
    except ClientError as e:
        print(e)

    return policy_arn


def create_airflow_role(airflow_iam, config, policy_arn):
    """
    Create necessary Role for AWS MWAA Airflow environment.
    
    Parameters:
    airflow_iam (boto3 client): boto3 client for creation role on AWS IAM
    config (CONFIG): configuration file variable objects
    policy_arn (string): policy arn string for attach policy to role
    """
    try:
        response = airflow_iam.create_role(RoleName=config["AWS_AIR_PRE"]["ROLE_NAME"],
                                           AssumeRolePolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": ["airflow-env.amazonaws.com", "ec2.amazonaws.com"]}, "Action": "sts:AssumeRole"}]}))

        response2 = airflow_iam.attach_role_policy(RoleName=config["AWS_AIR_PRE"]["ROLE_NAME"],
                                                   PolicyArn=policy_arn)
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200 and response2["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print("Created Airflow Role")
        else:
            print("Failed create Airflow Role.")
    except ClientError as e:
        print(e)


def create_airflow_vpc(airflow_vpc, config):
    """
    Create necessary VPC Stack for AWS MWAA Airflow environment.
    
    Parameters:
    airflow_vpc (boto3 client): boto3 client for creation vpc stack on AWS 
    config (CONFIG): configuration file variable objects
    """
    try:
        response = airflow_vpc.create_stack(StackName=config["AWS_AIR_PRE"]["VPC_STACK_NAME"],
                                            TemplateURL=config["AWS_AIR_PRE"]["VPC_STACK_TEMPLATE"],
                                            OnFailure='DELETE')
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print("Created Airflow VPC")
        else:
            print("Failed create Airflow VPC.")
    except ClientError as e:
        print(e)
    stack_name = config["AWS_AIR_PRE"]["VPC_STACK_NAME"]
    print("Waiting for VPC Stack {} to be created...".format(stack_name))
    is_created = False
    while not is_created:
        sleep(1)
        status_response = airflow_vpc.describe_stacks(StackName=stack_name)["Stacks"][0]
        is_created = status_response["StackStatus"] == "CREATE_COMPLETE"
    print("VPC Stack {} created.".format(stack_name))


def create_airflow_s3(airflow_s3, config):
    """
    Create necessary S3 Buckets for AWS MWAA Airflow environment.
    
    Parameters:
    airflow_s3 (boto3 client): boto3 client for creation s3 buckets on AWS S3
    config (CONFIG): configuration file variable objects
    """
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

            """response_plugin = airflow_s3.put_object(Bucket=config["AWS_AIR"]["BUCKET"], Key=(config["AWS_AIR"]["PLUGIN_PATH"] + '/'))
            if response_plugin["ResponseMetadata"]["HTTPStatusCode"] == 200:
                print("Created plugin path on: s3://{}".format(response["Location"]))
            else:
                print("Failed create_environment plugin path")"""
        else:
            print("Failed create_environment airflow bucket.")
    except ClientError as e:
        print(e)


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


policy_arn = create_airflow_policy(airflow_iam, CONFIG)
if policy_arn is not None:
    create_airflow_role(airflow_iam, CONFIG, policy_arn)
else:
    print("Create Policy failed. Role creation stopped.")

create_airflow_vpc(airflow_vpc, CONFIG)
create_airflow_s3(airflow_s3, CONFIG)
