# ann_load.py
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
#
# Exercises the annotator's auto scaling
#
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import json
import sys
import time
import uuid
from botocore.exceptions import ClientError

# Define constants here; no config file is used for this script
USER_ID = "7f34fee5-e7cb-48eb-a644-8f4c93f3a28c"
EMAIL = "zhoua@uchicago.edu"
QUEUE_NAME = "zhoua_a17_job_requests"  # job request queue

# Initialize SQS client
sqs = boto3.client('sqs','us-east-1')


def load_requests_queue():
    queue_url = "https://sqs.us-east-1.amazonaws.com/127134666975/zhoua_a17_job_requests"

    # Define job data
    job_data = {
        "user_id": USER_ID,
        "email": EMAIL,
        "job_id": str(uuid.uuid4()),  # Generate a unique job ID
        "input_file_name": "test_file.txt",
        "s3_inputs_bucket": "gas-inputs",
        "s3_key_input_file": "test_file.txt",
        "s3_results_bucket": "gas-results",
        "submit_time": int(time.time()),
        "job_status": "PENDING"
    }

    # Send message to request queue
    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(job_data)
        )
        print(f"Sent message ID: {response['MessageId']}")
    except ClientError as e:
        print(f"Error sending message to queue: {e}")

def main():
    while True:
        try:
            load_requests_queue()
            time.sleep(3)
        except ClientError as e:
            print(f"Irrecoverable error. Exiting: {e}")
            sys.exit(1)

if __name__ == "__main__":
    main()

# EOF
