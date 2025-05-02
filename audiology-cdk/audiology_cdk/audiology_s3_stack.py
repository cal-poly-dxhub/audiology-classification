from aws_cdk import (
    Stack,
    RemovalPolicy,
)
from constructs import Construct
from aws_cdk.aws_s3 import Bucket, BucketEncryption

class Audiologys3Stack(Stack):

    def __init__(self, scope: Construct, construct_id: str, *, bucket_name: str = None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = Bucket(
            self, "AudiologyS3",
            bucket_name=bucket_name,
            versioned=True,                # enable versioning
            encryption=BucketEncryption.S3_MANAGED,  # server-side encryption
            public_read_access=False,      # secure: no public read
            removal_policy=RemovalPolicy.RETAIN,
            auto_delete_objects=False
        )
        self.bucket = bucket
