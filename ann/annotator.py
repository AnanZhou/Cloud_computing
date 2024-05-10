import boto3
import os
import subprocess
from botocore.exceptions import NoCredentialsError, ClientError
from botocore.client import Config
import json
import logging
import configparser

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration file
config = configparser.ConfigParser()
config.read('annotator_config.ini')  # Ensure correct path

# Access configuration values
AWS_REGION = config['aws']['AwsRegionName']
QUEUE_URL = config['sqs']['RequestQueueUrl'] # request sqs
WAIT_TIME = int(config['sqs']['WaitTime'])
MAX_MESSAGES = int(config['sqs']['MaxMessages'])
INPUTS_BUCKET_NAME = config['s3']['InputsBucketName']
RESULTS_BUCKET_NAME = config['s3']['ResultsBucketName']
KEY_PREFIX = config['s3']['KeyPrefix']
ANNOTATIONS_TABLE = config['dynamodb']['AnnotationsTable']

# Initialize AWS clients and resources
sqs = boto3.resource('sqs', region_name=AWS_REGION)
queue = sqs.Queue(QUEUE_URL)
s3_client = boto3.client('s3', region_name=AWS_REGION, config=Config(signature_version='s3v4'))
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
table = dynamodb.Table(ANNOTATIONS_TABLE)

def process_message(message):
    try:
        sns_message = json.loads(message.body)
        data = json.loads(sns_message['Message']) if 'Type' in sns_message and sns_message['Type'] == 'Notification' else sns_message

        job_id = data['job_id']
        bucket_name = data['s3_inputs_bucket']
        key = data['s3_key_input_file']

        # Check the current status of the job in DynamoDB
        response = table.get_item(Key={'job_id': job_id})
        if 'Item' in response:
            status = response['Item']['job_status']
            if status in ['COMPLETED', 'RUNNING']:
                logging.info(f"Job is already {status}!\nInput file: {data['input_file_name']}\n job_id: {job_id}")
                message.delete()  # Delete the message from the queue
                return

        job_dir = os.path.join('./jobs', job_id)
        os.makedirs(job_dir, exist_ok=True)
        local_filename = os.path.join(job_dir, os.path.basename(key))

        # Download the file from S3
        s3_client.download_file(bucket_name, key, local_filename)

        # Launch the annotation process
        subprocess.Popen(['python', 'run.py', local_filename])

        # Update DynamoDB status to RUNNING
        table.update_item(
            Key={'job_id': job_id},
            UpdateExpression='SET job_status = :status',
            ExpressionAttributeValues={':status': 'RUNNING'}
        )
        
        # Successfully processed, delete the message
        message.delete()

    except ClientError as e:
        logging.error(f"Failed to download file or update DynamoDB: {str(e)}")
    except Exception as e:
        logging.error(f"Error processing message: {str(e)}")

# Main loop for message polling
while True:
    try:
        messages = queue.receive_messages(WaitTimeSeconds=WAIT_TIME, MaxNumberOfMessages=MAX_MESSAGES)
        for message in messages:
            process_message(message)
    except NoCredentialsError as e:
        logging.error("No AWS credentials found: " + str(e))
    except ClientError as e:
        logging.error("SQS client error occurred: " + str(e))
    except Exception as e:
        logging.error("Unexpected error during queue processing: " + str(e))