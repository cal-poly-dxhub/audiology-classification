from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    Duration,
    aws_s3 as s3,
    aws_s3_notifications as s3n, RemovalPolicy
)
import os
from aws_cdk.aws_s3 import Bucket, BucketEncryption
from constructs import Construct
cwd = os.getcwd()


class AudiologyAppStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, bucket_name: str,  **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.bucket = Bucket(
            self, "AudiologyS3",
            bucket_name=bucket_name,
            versioned=True,                # enable versioning
            encryption=BucketEncryption.S3_MANAGED,  # server-side encryption
            public_read_access=False,      # secure: no public read
            removal_policy=RemovalPolicy.RETAIN,
            auto_delete_objects=False
        )

        fn = _lambda.Function(
            self, "AudiologyHandler",
            runtime=_lambda.Runtime.PYTHON_3_10,
            handler="handler.handler",
            code=_lambda.Code.from_asset(os.path.join(cwd, "lambda")),  # directory with handler.py & deps
            memory_size=512,
            timeout=Duration.seconds(15),
            environment={
                "BUCKET_NAME": bucket_name,
            }
        )
        self.bucket.grant_read(fn)

        self.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED_PUT,
            s3n.LambdaDestination(fn),

            # only trigger when lab data is input to the bucket of type .json
            s3.NotificationKeyFilter(prefix="lab_data_input/", suffix=".json")
        )
