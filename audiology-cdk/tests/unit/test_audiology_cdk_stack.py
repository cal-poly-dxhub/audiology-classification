import aws_cdk as core
import aws_cdk.assertions as assertions

from audiology_cdk.audiology_cdk_stack import AudiologyCdkStack

# example tests. To run these tests, uncomment this file along with the example
# resource in audiology_cdk/audiology_s3_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = AudiologyCdkStack(app, "audiology-cdk")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
