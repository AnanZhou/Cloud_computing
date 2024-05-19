# thaw_app.py
#
# Thaws upgraded (Premium) user data
#
# Copyright (C) 2015-2024 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import json
import requests
import sys
import time
import configparser
import logging

from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr
from flask import Flask, request

app = Flask(__name__)
app.url_map.strict_slashes = False

# Load configurations from util_config.ini
config = configparser.ConfigParser()
config.read("../util_config.ini")  # path to util file

# Apply configurations from the INI file
app.config['AWS_GLACIER'] = config.get('glacier', 'VaultName')
app.config['S3_RESULTS_BUCKET'] = config.get('s3', 'ResultsBucketName')
app.config['SNS_TOPIC_ARN'] = config.get('sns', 'TopicArn')
app.config['DYNAMODB_TABLE_NAME'] = config.get('gas', 'AnnotationsTable')
app.config['AWS_REGION'] = config.get('aws', 'AwsRegionName')

# Get configuration and add to Flask app object
environment = "thaw_app_config.Config"
app.config.from_object(environment)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

@app.route("/", methods=["GET"])
def home():
    return "This is the Thaw utility: POST requests to /thaw."

# subscription SNS
# https://docs.aws.amazon.com/sns/latest/dg/sns-create-subscribe-endpoint-to-topic.html
@app.route("/thaw", methods=["POST"])
def thaw_premium_user_data():
    data = json.loads(request.data)
    
    # Handle SNS Subscription Confirmation
    if 'Type' in data and data['Type'] == 'SubscriptionConfirmation':
        subscribe_url = data['SubscribeURL']
        logger.info(f"Confirming SNS subscription: {subscribe_url}")
        response = requests.get(subscribe_url)
        logger.info(f"SNS subscription confirmed: {response.status_code}")
        return "Subscription confirmed", 200
    
    # Handle SNS Notification
    if 'Type' in data and data['Type'] == 'Notification':
        message = json.loads(data['Message'])  # SNS message structure contains 'Message' field
        user_id = message.get('user_id')

        if not user_id:
            logger.error("Missing user_id")
            return "Missing user_id", 400

        # Initialize DynamoDB and Glacier clients
        dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION'])
        table = dynamodb.Table(app.config['DYNAMODB_TABLE_NAME'])
        glacier = boto3.client('glacier', region_name=app.config['AWS_REGION'])
        sns_client = boto3.client('sns', region_name=app.config['AWS_REGION'])
        vault_name = app.config['AWS_GLACIER']
        sns_topic_arn = app.config['SNS_TOPIC_ARN']

        # Scan DynamoDB to get all archived files for the user_id with results_file_archive_id
        try:
            scan_response = table.scan(
                FilterExpression=Attr('user_id').eq(user_id) & Attr('results_file_archive_id').exists()
            )
        except ClientError as e:
            logger.error(f"Error scanning DynamoDB: {e}")
            return "Error scanning DynamoDB", 500

        items = scan_response.get('Items', [])

        for item in items:
            logger.info(f"Processing item: {item}")  # Log the entire item for debugging

            archive_id = item.get('results_file_archive_id')
            job_id = item.get('job_id')
            s3_key_result = item.get('s3_key_result_file')
            
            # Ensure both archive_id and job_id are present
            if not archive_id or not job_id:
                logger.error(f"Missing archive_id or job_id for user {user_id} in item: {item}")
                continue

            try:
                # Attempt expedited retrieval
                # https://docs.aws.amazon.com/cli/latest/reference/glacier/initiate-job.html
                response = glacier.initiate_job(
                    vaultName=vault_name,
                    jobParameters={
                        'Type': 'archive-retrieval',
                        'Tier': 'Expedited',
                        'ArchiveId': archive_id,
                        'Description': f"Expedited retrieval for user {user_id}, job ID: {job_id}",
                        'SNSTopic': sns_topic_arn
                    }
                )
                logger.info(f"Initiated expedited retrieval for user {user_id}, job ID: {job_id}")

                # Update DynamoDB job_status to RESTORING
                try:
                    table.update_item(
                        Key={'job_id': job_id},
                        UpdateExpression="SET job_status = :status",
                        ExpressionAttributeValues={':status': 'RESTORING'}
                    )
                    logger.info(f"Updated job status to RESTORING for job {job_id}")
                except ClientError as e:
                    logger.error(f"Error updating DynamoDB for job {job_id}: {e}")

            except ClientError as e:
                error_code = e.response['Error']['Code']
                logger.error(f"Expedited retrieval failed for user {user_id}, archive {archive_id}: {e}")

                if error_code == 'InsufficientCapacityException':
                    try:
                        # Attempt standard retrieval if expedited fails
                        # https://stackoverflow.com/questions/56229778/glacierselectnotavailable-glacier-select-retrievals-are-currently-not-available
                        response = glacier.initiate_job(
                            vaultName=vault_name,
                            jobParameters={
                                'Type': 'archive-retrieval',
                                'Tier': 'Standard',
                                'ArchiveId': archive_id,
                                'Description': f"Standard retrieval for user {user_id}, job ID: {job_id}",
                                'SNSTopic': sns_topic_arn
                            }
                        )
                        logger.info(f"Initiated standard retrieval for user {user_id}, job ID: {job_id}")

                        # Update DynamoDB job_status to RESTORING
                        try:
                            table.update_item(
                                Key={'job_id': job_id},
                                UpdateExpression="SET job_status = :status",
                                ExpressionAttributeValues={':status': 'RESTORING'}
                            )
                            logger.info(f"Updated job status to RESTORING for job {job_id}")
                        except ClientError as e:
                            logger.error(f"Error updating DynamoDB for job {job_id}: {e}")

                        # Publish message to SNS topic with complete job information (debug purpose)
                        sns_message = {
                            'user_id': user_id,
                            'archive_id': archive_id,
                            'job_id': job_id,
                            'StatusCode': 'Initiated'
                        }
                        sns_client.publish(
                            TopicArn=sns_topic_arn,
                            Message=json.dumps(sns_message),
                            Subject='Glacier Retrieval Initiated'
                        )

                    except ClientError as e:
                        logger.error(f"Standard retrieval also failed for user {user_id}, archive {archive_id}: {e}")
                        continue

        return "Thaw initiated for all user files", 200

    logger.error("Invalid SNS message")
    return "Invalid SNS message", 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002, debug=True)
