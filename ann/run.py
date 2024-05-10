import sys
import time
import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import subprocess
import driver
import configparser
import logging
import json

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration file
config = configparser.ConfigParser()
config.read('annotator_config.ini')

# Access configuration values
AWS_REGION = config.get('aws', 'AwsRegionName')
INPUTS_BUCKET_NAME = config.get('s3', 'InputsBucketName')
RESULTS_BUCKET_NAME = config.get('s3', 'ResultsBucketName')
KEY_PREFIX = config.get('s3', 'KeyPrefix')
ANNOTATIONS_TABLE = config.get('dynamodb', 'AnnotationsTable')
USER_ID = config.get('gas', 'user_id')
CNET_ID = config['DEFAULT']['CnetId']

# Reuslt SNS

SNS_Result_TOPIC_ARN = config['sns']['JobResultsTopic']


class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")

def update_dynamodb(job_id, data):
    """Update DynamoDB table with job completion details."""
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    table = dynamodb.Table(ANNOTATIONS_TABLE)
    try:
        response = table.update_item(
            Key={'job_id': job_id},
            UpdateExpression='SET s3_results_bucket = :rb, s3_key_result_file = :rf, s3_key_log_file = :lf, complete_time = :ct, job_status = :js',
            ExpressionAttributeValues={
                ':rb': data['s3_results_bucket'],
                ':rf': data['s3_key_result_file'],
                ':lf': data['s3_key_log_file'],
                ':ct': int(time.time()),
                ':js': 'COMPLETED'
            }
        )
        logging.info("DynamoDB update successful")
        return True
    except Exception as e:
        logging.error(f"Failed to update DynamoDB: {str(e)}")
        return False

def send_sns_notification(topic_arn, data):
    """Send a notification to SNS about job completion with detailed job information."""
    sns_client = boto3.client('sns', region_name=AWS_REGION)
    message = json.dumps({
        'job_id': data['job_id'],
        'user_id': data['user_id'],
        'input_file_name': data['input_file_name'],
        's3_inputs_bucket': data['s3_inputs_bucket'],
        's3_key_input_file': data['s3_key_input_file'],
        'complete_time': data['complete_time'],
        'job_status': data['job_status'],
        'message': 'Job completed successfully'
    })
    try:
        response = sns_client.publish(TopicArn=topic_arn, Message=message)
        logging.info(f"Notification sent to SNS. Message ID: {response['MessageId']}")
        return True
    except ClientError as e:
        logging.error(f"Failed to send SNS notification due to client error: {str(e)}")
        return False


def upload_file_to_s3(file_path, bucket, object_name):
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    try:
        s3_client.upload_file(file_path, bucket, object_name)
        logging.info(f"File {file_path} uploaded to {bucket}/{object_name}")
        return True
    except (NoCredentialsError, PartialCredentialsError, ClientError) as e:
        logging.error(f"Failed to upload {file_path}. Error: {str(e)}")
        return False

def delete_local_file(file_path):
    try:
        os.remove(file_path)
        logging.info(f"Deleted local file {file_path}")
    except OSError as e:
        logging.error(f"Error: {file_path} : {e.strerror}")

if __name__ == '__main__':
    if len(sys.argv) > 1:
        input_file_path = sys.argv[1].strip()
        with Timer():
            results_file = input_file_path.replace('.vcf', '.annot.vcf')
            log_file = input_file_path + '.count.log'

            driver.run(input_file_path, 'vcf')

        bucket_name = RESULTS_BUCKET_NAME
        cnet_id = CNET_ID
        user_prefix = USER_ID
        unique_id = os.path.basename(input_file_path).split('~')[0]

        s3_results_key = f"{cnet_id}/{user_prefix}/{unique_id}/{os.path.basename(results_file)}"
        s3_log_key = f"{cnet_id}/{user_prefix}/{unique_id}/{os.path.basename(log_file)}"
        
        # Upload results file to S3
        upload_file_to_s3(results_file, bucket_name, s3_results_key)
        
        # Upload log file to S3
        upload_file_to_s3(log_file, bucket_name, s3_log_key)

        # Update DynamoDB
        update_dynamodb(unique_id, {
            's3_results_bucket': bucket_name,
            's3_key_result_file': s3_results_key,
            's3_key_log_file': s3_log_key
        })

        # Prepare data for SNS notification
        notification_data = {
            'job_id': unique_id,
            'user_id': user_prefix,
            'input_file_name': os.path.basename(input_file_path),
            's3_inputs_bucket': bucket_name,
            's3_key_input_file': s3_results_key,
            'complete_time': int(time.time()),
            'job_status': 'COMPLETED'
        }

        # Send SNS notification to result Topic
        send_sns_notification(SNS_Result_TOPIC_ARN, notification_data)
        
        # Delete local files
        delete_local_file(results_file)
        delete_local_file(log_file)
    else:
        logging.error("A valid .vcf file must be provided as input to this program.")