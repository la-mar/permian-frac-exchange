# flake8: noqa
import json
import os

import boto3
import tomlkit


def get_project_meta() -> dict:
    pyproj_path = "./pyproject.toml"
    if os.path.exists(pyproj_path):
        with open(pyproj_path, "r") as pyproject:
            file_contents = pyproject.read()
        return tomlkit.parse(file_contents)["tool"]["poetry"]
    else:
        return {}


account_id = boto3.client("sts").get_caller_identity().get("Account")

pkg_meta = get_project_meta()
project = pkg_meta.get("name")
version = pkg_meta.get("version")

iam = boto3.client("iam")

path = "/"
role_name = f"{project}-task-role"
description = f"Task role for {project}"

trust_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {"Service": "ecs-tasks.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }
    ],
}

policy_name = f"{project}-task-policy"
policy_arn = f"arn:aws:iam::{account_id}:policy/{policy_name}"
policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "0",
            "Effect": "Allow",
            "Action": "ssm:GetParameter*",
            "Resource": [
                f"arn:aws:ssm:*:*:parameter/{project}/*",
                "arn:aws:ssm:*:*:parameter/datadog/*",
            ],
        },
        {
            "Sid": "1",
            "Effect": "Allow",
            "Action": [
                "kms:ListKeys",
                "kms:ListAliases",
                "kms:Describe*",
                "kms:Decrypt",
            ],
            "Resource": f"arn:aws:kms:us-east-1:{account_id}:key/{os.getenv('KMS_KEY_FOR_SSM')}",
        },
        {
            "Sid": "2",
            "Effect": "Allow",
            "Action": ["kms:Decrypt", "secretsmanager:GetSecretValue"],
            "Resource": [
                f"arn:aws:secretsmanager:us-east-1:{account_id}:secret:{os.getenv('DOCKER_SECRETS_MANAGER_ID')}",
                f"arn:aws:kms:us-east-1:{account_id}:key/{os.getenv('KMS_KEY_FOR_SECRETS_MANAGER')}",
            ],
        },
    ],
}

tags = [{"Key": "service_name", "Value": project}]

try:
    role = iam.create_role(
        Path=path,
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(trust_policy),
        Description=description,
        MaxSessionDuration=3600,
        Tags=tags,
    )
except:
    role = iam.get_role(RoleName=role_name)

try:
    policy_response = iam.create_policy(
        PolicyName=policy_name, PolicyDocument=json.dumps(policy)
    )
except:
    policy_response = iam.create_policy_version(
        PolicyArn=policy_arn, PolicyDocument=json.dumps(policy)
    )

iam.attach_role_policy(PolicyArn=policy_arn, RoleName=role_name)

