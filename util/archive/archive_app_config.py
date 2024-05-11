# archive_app_config.py
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
#
# Set app configuration options for archive utility
#
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import os

# Get the IAM username that was stashed at launch time
try:
    with open("/home/ubuntu/.launch_user", "r") as file:
        iam_username = file.read().replace("\n", "")
except FileNotFoundError as e:
    if "LAUNCH_USER" in os.environ:
        iam_username = os.environ["LAUNCH_USER"]
    else:
        # Unable to set username, so exit
        print("Unable to find launch user name in local file or environment!")
        raise e


class Config(object):

    CSRF_ENABLED = True

    AWS_REGION_NAME = "us-east-1"

    # AWS DynamoDB table
    AWS_DYNAMODB_ANNOTATIONS_TABLE = f"{iam_username}_annotations"
    
    #SQS Setting:result sqs
    SQS_URL= "https://sqs.us-east-1.amazonaws.com/127134666975/zhoua_a14_job_results"

    #SNS Setting archive sns
    Topic_ARN = "arn:aws:sns:us-east-1:127134666975:zhoua_a14_archive"


### EOF
