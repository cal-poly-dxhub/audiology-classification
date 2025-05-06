#!/usr/bin/env python3
import os

import aws_cdk as cdk

from audiology_cdk.audiology_app_stack import AudiologyAppStack

app = cdk.App()

stack = AudiologyAppStack(app,
                 "AudiologyAppStack",
                 bucket_name="audiology-s3-bucket",
                 )
app.synth()
