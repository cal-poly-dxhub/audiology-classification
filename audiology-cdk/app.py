#!/usr/bin/env python3
import os

import aws_cdk as cdk

from audiology_cdk.audiology_s3_stack import Audiologys3Stack
from audiology_cdk.audiology_lambda_stack import AudiologyLambdaStack

app = cdk.App()

s3 = Audiologys3Stack(app,
                 "AudiologyS3Stack",
                 bucket_name="audiology-s3-bucket",
                 )
_lambda = AudiologyLambdaStack(app,
                     "audiology-lambda",
                     bucket= s3.bucket,
                     )


app.synth()
