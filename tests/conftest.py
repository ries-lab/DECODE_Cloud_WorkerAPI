import secrets
import time
from typing import Any, Generator
from unittest.mock import MagicMock

import boto3
import botocore.exceptions
import pytest
import requests
from mypy_boto3_s3.literals import BucketLocationConstraintType
from sqlalchemy import Engine, MetaData, create_engine

from workerfacing_api.crud import job_tracking


@pytest.fixture(scope="module")
def monkeypatch_module() -> Generator[pytest.MonkeyPatch, Any, None]:
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(autouse=True, scope="module")
def patch_update_job(monkeypatch_module: pytest.MonkeyPatch) -> MagicMock:
    mock_update_job = MagicMock()
    monkeypatch_module.setattr(job_tracking, "update_job", mock_update_job)
    return mock_update_job


class RDSTestingInstance:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.rds_client = boto3.client("rds", "eu-central-1")
        self.ec2_client = boto3.client("ec2", "eu-central-1")
        self.add_ingress_rule()
        self.db_url = self.create_db_url()
        self.delete_db_tables()

    @property
    def engine(self) -> Engine:
        for _ in range(5):
            try:
                engine = create_engine(self.db_url)
                engine.connect()
                return engine
            except Exception:
                time.sleep(60)
        raise RuntimeError("Could not create engine.")

    @property
    def vpc_sg_rule_params(self) -> dict[str, Any]:
        return {
            "GroupName": "default",
            "IpPermissions": [
                {
                    "FromPort": 5432,
                    "ToPort": 5432,
                    "IpProtocol": "tcp",
                    "IpRanges": [
                        {
                            "CidrIp": f"{requests.get('http://checkip.amazonaws.com').text.strip()}/24"
                        }
                    ],
                }
            ],
        }

    def add_ingress_rule(self) -> None:
        try:
            self.ec2_client.authorize_security_group_ingress(**self.vpc_sg_rule_params)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "InvalidPermission.Duplicate":
                pass
            else:
                raise e

    def delete_db_tables(self) -> None:
        metadata = MetaData()
        engine = self.engine
        metadata.reflect(engine)
        metadata.drop_all(engine)

    def get_db_password(self) -> str:
        secret_name = "decode-cloud-tests-db-pwd"
        sm_client = boto3.client("secretsmanager", "eu-central-1")
        try:
            return sm_client.get_secret_value(SecretId=secret_name)["SecretString"]
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                secret = secrets.token_urlsafe(32)
                boto3.client("secretsmanager", "eu-central-1").create_secret(
                    Name=secret_name, SecretString=secret
                )
                return secret
            else:
                raise e

    def create_db_url(self) -> str:
        user = "postgres"
        password = self.get_db_password()
        try:
            response = self.rds_client.describe_db_instances(
                DBInstanceIdentifier=self.db_name
            )
        except self.rds_client.exceptions.DBInstanceNotFoundFault:
            while True:
                try:
                    self.rds_client.create_db_instance(
                        DBName=self.db_name,
                        DBInstanceIdentifier=self.db_name,
                        AllocatedStorage=20,
                        DBInstanceClass="db.t3.micro",
                        Engine="postgres",
                        MasterUsername=user,
                        MasterUserPassword=password,
                        DeletionProtection=False,
                        BackupRetentionPeriod=0,
                    )
                    break
                except self.rds_client.exceptions.DBInstanceAlreadyExistsFault:
                    pass
            while True:
                response = self.rds_client.describe_db_instances(
                    DBInstanceIdentifier=self.db_name
                )
                assert len(response["DBInstances"]) == 1
                if response["DBInstances"][0]["DBInstanceStatus"] == "available":
                    break
                else:
                    time.sleep(5)
        address = response["DBInstances"][0]["Endpoint"]["Address"]
        return f"postgresql://{user}:{password}@{address}:5432/{self.db_name}"

    def cleanup(self) -> None:
        self.delete_db_tables()
        self.ec2_client.revoke_security_group_ingress(**self.vpc_sg_rule_params)


class S3TestingBucket:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.region_name: BucketLocationConstraintType = "eu-central-1"
        self.s3_client = boto3.client(
            "s3",
            region_name=self.region_name,
            # required for pre-signing URLs to work
            endpoint_url=f"https://s3.{self.region_name}.amazonaws.com",
        )
        self.initialize_bucket()

    def cleanup(self) -> bool:
        """Returns True if bucket exists and all objects are deleted."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except self.s3_client.exceptions.NoSuchBucket:
            return False
        except self.s3_client.exceptions.ClientError:
            return False
        s3 = boto3.resource("s3", region_name=self.region_name)
        s3_bucket = s3.Bucket(self.bucket_name)
        bucket_versioning = s3.BucketVersioning(self.bucket_name)
        if bucket_versioning.status == "Enabled":
            s3_bucket.object_versions.delete()
        else:
            s3_bucket.objects.all().delete()
        return True

    def initialize_bucket(self) -> None:
        exists = self.cleanup()
        if not exists:
            self.s3_client.create_bucket(
                Bucket=self.bucket_name,
                CreateBucketConfiguration={"LocationConstraint": self.region_name},
            )
            self.s3_client.get_waiter("bucket_exists").wait(Bucket=self.bucket_name)
