import datetime
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

TEST_BUCKET_PREFIX = "decode-cloud-worker-api-tests-"
REGION_NAME: BucketLocationConstraintType = "eu-central-1"
# True for Aurora clusters, False for RDS instances
USE_AURORA_CLUSTERS = True


@pytest.fixture(scope="session")
def monkeypatch_module() -> Generator[pytest.MonkeyPatch, Any, None]:
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(autouse=True, scope="session")
def patch_update_job(monkeypatch_module: pytest.MonkeyPatch) -> MagicMock:
    mock_update_job = MagicMock()
    monkeypatch_module.setattr(job_tracking, "update_job", mock_update_job)
    return mock_update_job


class DatabaseTestingInstance:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.use_aurora = USE_AURORA_CLUSTERS

    def create(self) -> None:
        self.rds_client = boto3.client("rds", "eu-central-1")
        self.ec2_client = boto3.client("ec2", "eu-central-1")
        self.add_ingress_rule()
        self.db_url = self.create_db_url()
        self.engine = self.get_engine()
        self.delete_db_tables()

    def get_engine(self) -> Engine:
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

        if self.use_aurora:
            # Aurora cluster mode
            try:
                response_cluster = self.rds_client.describe_db_clusters(
                    DBClusterIdentifier=self.db_name
                )
            except self.rds_client.exceptions.DBClusterNotFoundFault:
                while True:
                    try:
                        self.rds_client.create_db_cluster(
                            DatabaseName=self.db_name,
                            DBClusterIdentifier=self.db_name,
                            Engine="aurora-postgresql",
                            EngineMode="provisioned",
                            ServerlessV2ScalingConfiguration={
                                "MinCapacity": 0,
                                "MaxCapacity": 1,
                            },
                            MasterUsername=user,
                            MasterUserPassword=password,
                            DeletionProtection=False,
                            EnableCloudwatchLogsExports=[],
                        )
                        break
                    except self.rds_client.exceptions.DBClusterAlreadyExistsFault:
                        pass

                max_wait_time = 600  # 10 minutes
                start_time = time.time()
                while True:
                    if time.time() - start_time > max_wait_time:
                        raise TimeoutError(
                            f"Aurora cluster {self.db_name} did not become available within {max_wait_time} seconds"
                        )
                    response_cluster = self.rds_client.describe_db_clusters(
                        DBClusterIdentifier=self.db_name
                    )
                    assert len(response_cluster["DBClusters"]) == 1
                    if response_cluster["DBClusters"][0]["Status"] == "available":
                        break
                    else:
                        time.sleep(5)

                try:
                    self.rds_client.describe_db_instances(
                        DBInstanceIdentifier=self.db_name
                    )
                except self.rds_client.exceptions.DBInstanceNotFoundFault:
                    while True:
                        try:
                            self.rds_client.create_db_instance(
                                DBInstanceIdentifier=self.db_name,
                                DBClusterIdentifier=self.db_name,
                                DBInstanceClass="db.serverless",
                                Engine="aurora-postgresql",
                            )
                            break
                        except self.rds_client.exceptions.DBInstanceAlreadyExistsFault:
                            pass
                    max_wait_time = 600  # 10 minutes
                    start_time = time.time()
                    while True:
                        if time.time() - start_time > max_wait_time:
                            raise TimeoutError(
                                f"Aurora instance {self.db_name} did not become available within {max_wait_time} seconds"
                            )
                        response_instance = self.rds_client.describe_db_instances(
                            DBInstanceIdentifier=self.db_name
                        )
                        assert len(response_instance["DBInstances"]) == 1
                        if (
                            response_instance["DBInstances"][0]["DBInstanceStatus"]
                            == "available"
                        ):
                            break
                        time.sleep(5)

            # Get the cluster endpoint
            address = response_cluster["DBClusters"][0]["Endpoint"]
        else:
            # RDS instance mode
            try:
                response_instance = self.rds_client.describe_db_instances(
                    DBInstanceIdentifier=self.db_name
                )
            except self.rds_client.exceptions.DBInstanceNotFoundFault:
                while True:
                    try:
                        self.rds_client.create_db_instance(
                            DBName=self.db_name,
                            DBInstanceIdentifier=self.db_name,
                            AllocatedStorage=20,
                            DBInstanceClass="db.t4g.micro",
                            Engine="postgres",
                            MasterUsername=user,
                            MasterUserPassword=password,
                            DeletionProtection=False,
                            BackupRetentionPeriod=0,
                            MultiAZ=False,
                            EnablePerformanceInsights=False,
                        )
                        break
                    except self.rds_client.exceptions.DBInstanceAlreadyExistsFault:
                        pass
                while True:
                    response_instance = self.rds_client.describe_db_instances(
                        DBInstanceIdentifier=self.db_name
                    )
                    assert len(response_instance["DBInstances"]) == 1
                    if (
                        response_instance["DBInstances"][0]["DBInstanceStatus"]
                        == "available"
                    ):
                        break
                    else:
                        time.sleep(5)
            address = response_instance["DBInstances"][0]["Endpoint"]["Address"]

        return f"postgresql://{user}:{password}@{address}:5432/{self.db_name}"

    def cleanup(self) -> None:
        self.delete_db_tables()
        self.ec2_client.revoke_security_group_ingress(**self.vpc_sg_rule_params)

    def delete(self) -> None:
        # never used (AWS tests skipped)
        if not hasattr(self, "rds_client"):
            return

        if self.use_aurora:
            # Delete Aurora instance first, then cluster
            instance_identifier = f"{self.db_name}-instance"
            try:
                self.rds_client.delete_db_instance(
                    DBInstanceIdentifier=instance_identifier,
                    SkipFinalSnapshot=True,
                )
                # Wait for instance deletion before deleting cluster
                while True:
                    try:
                        self.rds_client.describe_db_instances(
                            DBInstanceIdentifier=instance_identifier
                        )
                        time.sleep(5)
                    except self.rds_client.exceptions.DBInstanceNotFoundFault:
                        break
            except self.rds_client.exceptions.DBInstanceNotFoundFault:
                pass

            self.rds_client.delete_db_cluster(
                DBClusterIdentifier=self.db_name,
                SkipFinalSnapshot=True,
            )
        else:
            self.rds_client.delete_db_instance(
                DBInstanceIdentifier=self.db_name,
                SkipFinalSnapshot=True,
                DeleteAutomatedBackups=True,
            )


class S3TestingBucket:
    def __init__(self, bucket_name_suffix: str):
        # S3 bucket names must be globally unique - avoid collisions by adding suffix
        self.bucket_name = f"{TEST_BUCKET_PREFIX}-{bucket_name_suffix}"
        self.region_name: BucketLocationConstraintType = REGION_NAME

    def create(self) -> None:
        self.s3_client = boto3.client(
            "s3",
            region_name=self.region_name,
            # required for pre-signing URLs to work
            endpoint_url=f"https://s3.{self.region_name}.amazonaws.com",
        )
        exists = self.cleanup()
        if not exists:
            self.s3_client.create_bucket(
                Bucket=self.bucket_name,
                CreateBucketConfiguration={"LocationConstraint": self.region_name},
            )
            self.s3_client.get_waiter("bucket_exists").wait(Bucket=self.bucket_name)

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

    def delete(self) -> None:
        # never used (AWS tests skipped)
        if not hasattr(self, "s3_client"):
            return
        exists = self.cleanup()
        if exists:
            self.s3_client.delete_bucket(Bucket=self.bucket_name)


@pytest.fixture(scope="session")
def database_testing_instance() -> Generator[DatabaseTestingInstance, Any, None]:
    # tests themselves must create the database by calling instance.create();
    # this way, if no test that needs the DB is run, no database is created
    # instance.delete() only deletes the database if it was created
    # Uses Aurora clusters if USE_AURORA_CLUSTERS=True, RDS instances if False
    instance = DatabaseTestingInstance("decodecloudintegrationtestsworkerapi")
    yield instance
    instance.delete()


@pytest.fixture(scope="session")
def s3_testing_bucket() -> Generator[S3TestingBucket, Any, None]:
    # tests themselves must create the bucket by calling bucket.create();
    # this way, if no test that needs the bucket is run, no S3 bucket is created
    # bucket.delete() only deletes the S3 bucket if it was created
    bucket_suffix = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d%H%M%S")
    bucket = S3TestingBucket(bucket_suffix)
    yield bucket
    bucket.delete()


@pytest.mark.aws
@pytest.fixture(scope="session", autouse=True)
def cleanup_old_test_buckets() -> None:
    """
    Find and delete all S3 buckets with the test prefix.
    This helps clean up buckets from previous test runs.
    """
    s3_client = boto3.client("s3", region_name=REGION_NAME)
    response = s3_client.list_buckets(Prefix=TEST_BUCKET_PREFIX)
    for bucket in response["Buckets"]:
        bucket_name = bucket["Name"]
        s3 = boto3.resource("s3", region_name=REGION_NAME)
        s3_bucket = s3.Bucket(bucket_name)
        bucket_versioning = s3.BucketVersioning(bucket_name)
        if bucket_versioning.status == "Enabled":
            s3_bucket.object_versions.delete()
        else:
            s3_bucket.objects.all().delete()
        s3_client.delete_bucket(Bucket=bucket_name)
