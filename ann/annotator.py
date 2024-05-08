import boto3
import os
import subprocess
from botocore.exceptions import NoCredentialsError, ClientError
from botocore.client import Config
import json
# improve error handling usiong logging
# https://www.geeksforgeeks.org/how-to-log-a-python-exception/
import logging
import configparser

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration file
config = configparser.ConfigParser()
config.read('annotator_config.ini') ## path 

# access configuration values
AWS_REGION = config['aws']['AwsRegionName']
QUEUE_URL = config['sqs']['QueueUrl']
WAIT_TIME = int(config['sqs']['WaitTime'])
MAX_MESSAGES = int(config['sqs']['MaxMessages'])
INPUTS_BUCKET_NAME = config['s3']['InputsBucketName']
RESULTS_BUCKET_NAME = config['s3']['ResultsBucketName']
KEY_PREFIX = config['s3']['KeyPrefix']
ANNOTATIONS_TABLE = config['dynamodb']['AnnotationsTable']

# Define directories for input files and job information
INPUT_FILE_DIR = os.path.expanduser('~/gas/ann/anntools/data')
JOB_INFO_DIR = './jobs'

# Ensure directories exist
try:
    os.makedirs(INPUT_FILE_DIR, exist_ok=True)
except OSError as e:
    logging.error(f"Failed to create directory {INPUT_FILE_DIR}: {str(e)}")
    raise

try:
    os.makedirs(JOB_INFO_DIR, exist_ok=True)
except OSError as e:
    logging.error(f"Failed to create directory {JOB_INFO_DIR}: {str(e)}")
    raise

# Set up AWS resources using config values
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html

sqs = boto3.resource('sqs', region_name=AWS_REGION)
queue = sqs.Queue(QUEUE_URL)
s3_client = boto3.client('s3', region_name=AWS_REGION, config=Config(signature_version='s3v4'))
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
table = dynamodb.Table(ANNOTATIONS_TABLE)


def process_message(message):
    try:
        sns_message = json.loads(message.body)
        
        # Check if it's an SNS Notification and extract the actual message
        # https://docs.aws.amazon.com/ses/latest/dg/notification-examples.html
        if 'Message' in sns_message and 'Type' in sns_message and sns_message['Type'] == 'Notification':
            data = json.loads(sns_message['Message'])
        else:
            data = sns_message
        
        job_id = data['job_id']
        bucket_name = data['s3_inputs_bucket']
        key = data['s3_key_input_file']

        job_dir = os.path.join(JOB_INFO_DIR, job_id)
        try:
            os.makedirs(job_dir, exist_ok=True)
        except OSError as e:
            logging.error(f"Failed to create directory {job_dir}: {str(e)}")
            raise

        local_filename = os.path.join(job_dir, key.split('/')[-1])
        s3_client.download_file(bucket_name, key, local_filename)

        ann_process = subprocess.Popen(['python', 'run.py', local_filename])
        
        
        # Update job status in DynamoDB
        # https://docs.python.org/3/library/asyncio-subprocess.html#asyncio.subprocess.Process.returncode
        if ann_process.returncode == 0:
            status = 'COMPLETED'
        else:
            status = 'FAILED'
            logging.warning(f"Annotation process for job {job_id} exited with return code {ann_process.returncode}")

        table.update_item(
            Key={'job_id': job_id},
            UpdateExpression='SET job_status = :status',
            ExpressionAttributeValues={':status': status}
        )
        
         # Successfully processed, delete the message
        message.delete()

    except ClientError as e:
        logging.error(f"Failed to download file or update DynamoDB: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Error processing message: {str(e)}")


# Poll the message queue in a loop using long polling
# https://stackoverflow.com/questions/76498541/optimal-method-to-long-poll-keep-on-retrieving-new-sqs-messages
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