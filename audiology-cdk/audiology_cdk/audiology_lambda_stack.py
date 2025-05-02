from aws_cdk import (
    Stack,
    aws_lambda as _lambda, Duration
)
import os
from constructs import Construct
cwd = os.getcwd()

class AudiologyLambdaStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, bucket, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        fn = _lambda.Function(
            self, "AudiologyHandler",
            runtime=_lambda.Runtime.PYTHON_3_10,
            handler="handler.handler",
            code=_lambda.Code.from_asset(os.path.join(cwd, "lambda")),  # directory with handler.py & deps
            memory_size=512,
            timeout=Duration.seconds(15),
            environment={
                "BUCKET_NAME": bucket.bucket_name,
            }
        )
