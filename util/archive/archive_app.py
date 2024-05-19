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
import logging

app = Flask(__name__)
app.url_map.strict_slashes = False

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Load configurations from util_config.ini
config = configparser.ConfigParser()
config.read("../util_config.ini")  # path to util file

# Apply configurations from the INI file
app.config['AWS_GLACIER'] = config.get('glacier','VaultName')
app.config['S3_RESULTS_BUCKET'] = config.get('s3', 'ResultsBucketName')

# Get configuration and add to Flask app object
environment = "archive_app_config.Config"
app.config.from_object(environment)

REGION_NAME = config.get('aws','AwsRegionName')

@app.route("/", methods=["GET"])
def home():
    return "This is the Archive utility: POST requests to /archive."

@app.route("/archive", methods=["POST"])
def archive_free_user_data():

    # Manual parsing of request data as JSON due to SNS using 'text/plain' content type
    # https://docs.aws.amazon.com/sns/latest/dg/sns-message-and-json-formats.html
    try:
        sns_message = json.loads(request.data.decode('utf-8'))
        logging.info("Received message: %s", json.dumps(sns_message, indent=4))
    except json.JSONDecodeError:
        return jsonify({'error': 'Invalid JSON'}), 400
    
    # SNS subscrption
    # https://docs.aws.amazon.com/sns/latest/dg/sns-subscribe-https-s-endpoints-to-topic.html.

    if sns_message.get('Type') == 'SubscriptionConfirmation':
        # Confirm the subscription
        token = sns_message['Token']
        topic_arn = sns_message['TopicArn']
        sns_client = boto3.client('sns', region_name=REGION_NAME)
        sns_client.confirm_subscription(
            TopicArn=topic_arn,
            Token=token
        )

        response_message = {'message': 'Subscription confirmed'}
        logging.info("Subscription confirmed for TopicArn: %s", sns_message['TopicArn'])
        return jsonify(response_message), 200
    
    elif sns_message.get('Type') == 'Notification':
        # Process the notification message
        response_message = {'message': 'Notification received and processed'}
        logging.info("Processing notification: %s", sns_message.get('Message'))
        message_contents = json.loads(sns_message['Message'])
        

        job_id = message_contents['job_id']
        user_status = message_contents['user_status']
        
        if user_status == 'free_user':
            # Retrieve job details from DynamoDB
            dynamodb_client = boto3.client('dynamodb', region_name=REGION_NAME)
            job_item = dynamodb_client.get_item(
                TableName=app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'],
                Key={'job_id': {'S': job_id}}
            )
            s3_bucket = app.config['S3_RESULTS_BUCKET']
            s3_key = job_item['Item']['s3_key_result_file']['S']

            # Process the message (move to Glacier)
            print(f"Archiving job {job_id}")
            archive_id = upload_to_glacier(s3_bucket, s3_key)

            # Update DynamoDB with Glacier archive ID
            # https://docs.aws.amazon.com/sns/latest/dg/example_cross_SubmitDataApp_section.html
            dynamodb_client.update_item(
                TableName= app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'],
                Key={'job_id': {'S': job_id}},
                UpdateExpression='SET results_file_archive_id = :val',
                ExpressionAttributeValues={':val': {'S': archive_id}}
            )
            return jsonify({'message': 'Archived successfully'}), 200

        else:
            return jsonify({'message': 'No action needed, user is premium'}), 200
    else:
        return jsonify({'message': 'Invalid SNS message type'}), 400

def upload_to_glacier(bucket, key):
    # upload to glacier
    # https://stackoverflow.com/questions/59039076/how-to-upload-a-file-to-amazon-glacier-deep-archive-using-boto3
    s3_client = boto3.client('s3', region_name=REGION_NAME)
    glacier_client = boto3.client('glacier', region_name=REGION_NAME)

    try:
        # Retrieve the object from S3
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()

        # Upload to Glacier
        response = glacier_client.upload_archive(vaultName=app.config['AWS_GLACIER'], body=body)
        if response['ResponseMetadata']['HTTPStatusCode'] == 201:
            print("Archive uploaded to Glacier, archive ID:", response['archiveId'])
            # Delete the file from S3
            s3_client.delete_object(Bucket=bucket, Key=key)
            print("Deleted from S3:", key)
        else:
            print("Failed to upload to Glacier, not deleting from S3.")
            print("Response:", response)
        return response['archiveId']
    except Exception as e:
        # Log the exception to understand what went wrong
        logging.error("Exception occurred while uploading to Glacier: %s", str(e))
        print("Failed to upload to Glacier, not deleting from S3.")
        return None
    
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)

### EOF