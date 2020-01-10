# flake8: noqa

"""
Example docker deployment to AWS ECS cluster.

The script does the following:

    1. Loads environment variables from .env file in the project root

    For each service in TASKS
    2. Generates a populated ECS task definition
        - You can configure your task definitions in the get_task_definition() method.
    3. Optionally authenticate Docker to ECR
    4. Optionally build any configured containers
    5. Optionally push any configured containers to ECR
    6. Register the new task definition in ECR
    7. Retrieve the latest task definition revision number
    8. Update the running service with the new task definition and force a new deployment
"""


import os
from typing import List

import boto3
import tomlkit
from dotenv import dotenv_values, load_dotenv

load_dotenv(".env")


def get_project_meta() -> dict:
    pyproj_path = "./pyproject.toml"
    if os.path.exists(pyproj_path):
        with open(pyproj_path, "r") as pyproject:
            file_contents = pyproject.read()
        return tomlkit.parse(file_contents)["tool"]["poetry"]
    else:
        return {}


pkg_meta = get_project_meta()
project = pkg_meta.get("name")
version = pkg_meta.get("version")

ENV = os.getenv("ENV", "prod")
AWS_ACCOUNT_ID = os.getenv(
    "AWS_ACCOUNT_ID", boto3.client("sts").get_caller_identity().get("Account")
)

IMAGE_TAG: str = os.getenv("IMAGE_TAG")  # type: ignore
IMAGE_NAME: str = f"{os.getenv('IMAGE_NAME')}{':' if IMAGE_TAG else ''}{IMAGE_TAG or ''}"

CLUSTER_NAME = os.getenv("CLUSTER_NAME")  # type: ignore
CLUSTER_ARN = f"arn:aws:ecs:us-east-1:{AWS_ACCOUNT_ID}:cluster/{CLUSTER_NAME}"
TASK_IAM_ROLE = f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/{project}-task-role"

if not all([ENV, AWS_ACCOUNT_ID, IMAGE_NAME, CLUSTER_NAME]):
    raise ValueError("One or more environment variables are missing")


TASKS: List[dict] = [
    {
        "service": "frac-schedule-sync",
        "command": "fracx run collector",
        "rule": "schedule-10pm",
    }
]


TAGS = [
    {"key": "domain", "value": "engineering"},
    {"key": "service_name", "value": project},
    {"key": "environment", "value": ENV},
    {"key": "terraform", "value": "false"},
]

print("\n\n" + "-" * 30)
print(f"ENV: {ENV}")
print(f"AWS_ACCOUNT_ID: {AWS_ACCOUNT_ID}")
print(f"CLUSTER_NAME: {CLUSTER_NAME}")
print(f"TASKS: {TASKS}")
print("-" * 30 + "\n\n")


def get_task_definition(
    task_family: str,
    image: str,
    command: str,
    container_name: str = None,
    tags: list = [],
    task_iam_role_arn: str = "ecsTaskExecutionRole",
):
    task_def = {
        "containerDefinitions": [
            {
                "name": container_name or task_family,
                "command": command.split(" "),
                "memoryReservation": 256,
                "cpu": 256,
                "image": image,
                "repositoryCredentials": {
                    "credentialsParameter": f"arn:aws:secretsmanager:us-east-1:{AWS_ACCOUNT_ID}:secret:{os.getenv('DOCKER_SECRETS_MANAGER_ID')}"
                },
                "essential": True,
            },
        ],
        "executionRoleArn": "ecsTaskExecutionRole",
        "family": task_family,
        "networkMode": "bridge",
        "taskRoleArn": task_iam_role_arn,
        "tags": tags,
    }

    return task_def


class AWSClient:
    access_key_id = None
    secret_access_key = None
    session_token = None
    account_id = None
    region = None
    _ecs = None

    def __init__(self):
        self.credentials()

    @property
    def has_credentials(self):
        return all(
            [
                self.access_key_id is not None,
                self.secret_access_key is not None,
                self.region is not None,
                self.account_id is not None,
            ]
        )

    @property
    def ecr_url(self):
        if not self.has_credentials:
            self.credentials()
        return f"{self.account_id}.dkr.ecr.{self.region}.amazonaws.com"

    def credentials(self):
        credentials = {
            "access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "region": os.getenv("AWS_REGION", "us-east-1"),
            "account_id": os.getenv("AWS_ACCOUNT_ID"),
            "session_token": os.getenv("AWS_SESSION_TOKEN"),
            "security_token": os.getenv("AWS_SECURITY_TOKEN"),
        }
        # pylint: disable=expression-not-assigned
        [setattr(self, k, v) for k, v in credentials.items()]  # type: ignore

        return credentials

    def get_client(self, service_name: str):

        if not self.has_credentials:
            self.credentials()

        return boto3.client(
            service_name,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name=self.region,
            aws_session_token=self.session_token,
        )

    @property
    def ecs(self):
        return self._ecs or self.get_client("ecs")

    def get_latest_revision(self, task_name: str):
        response = self.ecs.describe_task_definition(taskDefinition=task_name)
        return response["taskDefinition"]["revision"]


client = AWSClient()
events = client.get_client("events")
target_id = 0
targets = []


for task in TASKS:
    service = task["service"]
    command = task["command"]
    rule = task["rule"]
    print(service + ":")
    try:
        prev_rev_num = client.get_latest_revision(service)
    except:  # pylint: disable=bare-except
        prev_rev_num = "?"
    cdef = get_task_definition(
        task_family=service,
        image=IMAGE_NAME,
        command=command,
        tags=TAGS,
        task_iam_role_arn=TASK_IAM_ROLE,
    )

    task_def_arn = client.ecs.register_task_definition(**cdef)["taskDefinition"][
        "taskDefinitionArn"
    ]

    rev_num = client.get_latest_revision(service)
    print("\t" + f"updated revision: {prev_rev_num} -> {rev_num}")
    task_count = 1
    targets = [
        {
            "Id": str(target_id),
            "Arn": CLUSTER_ARN,
            "RoleArn": f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/ecsEventsRole",
            "EcsParameters": {
                "TaskDefinitionArn": task_def_arn,
                "TaskCount": task_count,
            },
        }
    ]
    response = events.put_targets(Rule=rule, Targets=targets)
    print("\t" + f"created event: {CLUSTER_NAME}/{service} - {rule}")
    target_id += 1


print("\n\n")
