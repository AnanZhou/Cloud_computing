# archive_app.py
#
# Archive Free user data
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import json
import requests
import sys
import time
import configparser
from flask import Flask, request,jsonify

app = Flask(__name__)
app.url_map.strict_slashes = False

# Load configurations from util_config.ini
config = configparser.ConfigParser()
config.read("../util_config.ini")  # path to util file

# Apply configurations from the INI file
app.config['SQS_WAIT_TIME'] = config.getint('sqs', 'WaitTime', fallback=20)
app.config['SQS_MAX_MESSAGES'] = config.getint('sqs', 'MaxMessages', fallback=10)
app.config['AWS_GLACIER'] = config.get('glacier','VaultName')

# Get configuration and add to Flask app object
environment = "archive_app_config.Config"
app.config.from_object(environment)


@app.route("/", methods=["GET"])
def home():
    return "This is the Archive utility: POST requests to /archive."

def upload_to_glacier(bucket, key):
    s3_client = boto3.client('s3', region_name=app.config['AWS_REGION_NAME'])
    glacier_client = boto3.client('glacier', region_name=app.config['AWS_REGION_NAME'])

    # Get the S3 object
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = obj['Body'].read()

    # Upload to Glacier
    response = glacier_client.upload_archive(vaultName=app.config['AWS_GLACIER'], body=body)
    return response['archiveId']


@app.route("/archive", methods=["POST"])
def archive_free_user_data():
    # Initialize AWS clients
    sqs_client = boto3.client('sqs', region_name=app.config['AWS_REGION_NAME'])
    dynamodb_client = boto3.client('dynamodb', region_name=app.config['AWS_REGION_NAME'])

    # Extract the message type and contents
    sns_message = request.get_json()

    # Handle different types of SNS messages
    if sns_message.get('Type') == 'SubscriptionConfirmation':
        # Confirm the subscription
        sns_client = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
        token = sns_message['Token']
        topic_arn = sns_message['TopicArn']
        response = sns_client.confirm_subscription(
            TopicArn=topic_arn,
            Token=token,
            AuthenticateOnUnsubscribe='true'
        )
        return jsonify({'message': 'Subscription confirmed'}), 200

    elif sns_message.get('Type') == 'Notification':
        # Process the notification message
        message_contents = json.loads(sns_message['Message'])
        job_id = message_contents['job_id']
        user_status = message_contents['user_status']
        
        if user_status == 'free':
            # Retrieve message from SQS
            response = sqs_client.receive_message(
                QueueUrl=app.config['SQS_URL'],  # Ensure the queue URL is specified in the config
                MaxNumberOfMessages=1
            )
            if 'Messages' in response:
                message = response['Messages'][0]
                receipt_handle = message['ReceiptHandle']
                job_id = message['Body']  # Assuming job ID is directly in the message body

                #Get details from DynamoDB to find the S3 object key
                dynamodb_client = boto3.client('dynamodb', region_name=app.config['AWS_REGION_NAME'])
                job_item = dynamodb_client.get_item(TableName=app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'], Key={'job_id': {'S': job_id}})
                s3_bucket = job_item['Item']['s3_results_bucket']['S']
                s3_key = job_item['Item']['s3_key_result_file']['S']
                # Process the message (e.g., move to Glacier) should move!!
                print(f"Archiving job {job_id}")
                # Upload to Glacier
                archive_id = upload_to_glacier(s3_bucket, s3_key)

                # Delete message from the queue after processing
                sqs_client.delete_message(
                    QueueUrl=app.config['SQS_URL'],
                    ReceiptHandle=receipt_handle
                )

                # Update DynamoDB with Glacier archive ID
                dynamodb_client.update_item(
                    TableName=app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'],
                    Key={'job_id': {'S': job_id}},
                    UpdateExpression='SET results_file_archive_id = :val',
                    ExpressionAttributeValues={':val': {'S': archive_id}}
                )
                return jsonify({'message': 'Archived successfully'}), 200
            else:
                return jsonify({'message': 'No messages in queue'}), 200
        else:
            return jsonify({'message': 'No action needed, user is premium'}), 200
    else:
        return jsonify({'message': 'Invalid SNS message type'}), 400

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)

### EOF
