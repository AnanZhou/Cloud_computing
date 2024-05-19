# restore.py
#
# Restores thawed data, saving objects to S3 results bucket
# NOTE: This code is for an AWS Lambda function
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
##

import boto3
import json
import logging
import os
import tempfile
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Global configuration variables
AWS_REGION = os.environ['MY_APPAWS_REGION']
GLACIER_VAULT_NAME = os.environ['GLACIER_VAULT_NAME']
GAS_RESULTS_BUCKET = os.environ['GAS_RESULTS_BUCKET']
DYNAMODB_TABLE_NAME = os.environ['DYNAMODB_TABLE_NAME']

s3 = boto3.client('s3', region_name=AWS_REGION)
glacier = boto3.client('glacier', region_name=AWS_REGION)
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

def upload_file_to_s3(file_path, bucket, object_name):
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    try:
        s3_client.upload_file(file_path, bucket, object_name)
        logging.info(f"File {file_path} uploaded to {bucket}/{object_name}")
        return True
    except (NoCredentialsError, PartialCredentialsError, ClientError) as e:
        logging.error(f"Failed to upload {file_path}. Error: {str(e)}")
        return False

def get_s3_key_from_dynamodb(job_id):
    try:
        response = table.get_item(Key={'job_id': job_id})
        if 'Item' in response:
            return response['Item'].get('s3_key_result_file')
        else:
            logger.error(f"Job ID {job_id} not found in DynamoDB")
            return None
    except ClientError as e:
        logger.error(f"Error retrieving job ID {job_id} from DynamoDB: {e}")
        return None

def lambda_handler(event, context):
    for record in event['Records']:
        sns_message = json.loads(record['Sns']['Message'])

        # Extract relevant fields from the SNS message
        glacier_job_id = sns_message.get('JobId')
        archive_id = sns_message.get('ArchiveId')
        status_code = sns_message.get('StatusCode')
        user_id = None
        custom_job_id = None

        # Extract user_id and custom job ID from JobDescription if possible
        job_description = sns_message.get('JobDescription')
        if job_description:
            if 'user ' in job_description:
                user_id = job_description.split('user ')[1].split(',')[0]
            if 'job ID: ' in job_description:
                custom_job_id = job_description.split('job ID: ')[1].split(',')[0]
        
        if not all([glacier_job_id, user_id, archive_id, status_code, custom_job_id]):
            logger.error(f"Missing glacier_job_id, user_id, archive_id, status_code, or custom_job_id in SNS message: {sns_message}")
            continue

        if status_code != 'Succeeded':
            logger.info(f"Job {glacier_job_id} for user {user_id} has not succeeded yet.")
            continue

        try:
            # Get the temporary S3 object location
            job_output = glacier.get_job_output(vaultName=GLACIER_VAULT_NAME, jobId=glacier_job_id)
            temp_file_content = job_output['body'].read()

            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(temp_file_content)
                temp_file_path = temp_file.name

            # Get the s3_key_result_file from DynamoDB
            restored_key = get_s3_key_from_dynamodb(custom_job_id)
            if not restored_key:
                logger.error(f"Failed to get S3 key result file for job {custom_job_id}")
                continue

            # Upload the object to the gas-results S3 bucket using upload_file_to_s3
            if upload_file_to_s3(temp_file_path, GAS_RESULTS_BUCKET, restored_key):
                logger.info(f"Restored object for user {user_id} has been copied to {GAS_RESULTS_BUCKET}/{restored_key}")

                # Delete the Glacier archive
                glacier.delete_archive(vaultName=GLACIER_VAULT_NAME, archiveId=archive_id)
                logger.info(f"Glacier archive {archive_id} has been deleted.")

                # Update the DynamoDB table to reflect the restored state
                response = table.update_item(
                    Key={'job_id': custom_job_id},
                    UpdateExpression="SET job_status = :status, restored_key = :key",
                    ExpressionAttributeValues={
                        ':status': 'RESTORED',
                        ':key': restored_key
                    }
                )
                logger.info(f"DynamoDB table updated for job {custom_job_id}")
            else:
                logger.error(f"Failed to upload restored object for user {user_id} to S3")

            # Clean up the temporary file
            os.remove(temp_file_path)

        except ClientError as e:
            logger.error(f"Error restoring object for user {user_id}: {e}")

    return {
        'statusCode': 200,
        'body': 'SNS message processed.'
    }

### EOF
