import sys
import time
import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import subprocess
import driver
import configparser

#Load configuration file
# https://docs.python.org/3/library/configparser.html
config = configparser.ConfigParser()
config.read('annotator_config.ini')

# Access configuration values
# https://docs.python.org/3/library/configparser.html
AWS_REGION = config.get('aws', 'AwsRegionName')
INPUTS_BUCKET_NAME = config.get('s3', 'InputsBucketName')
RESULTS_BUCKET_NAME = config.get('s3', 'ResultsBucketName')
KEY_PREFIX = config.get('s3', 'KeyPrefix')
ANNOTATIONS_TABLE = config.get('dynamodb', 'AnnotationsTable')
USER_ID = config.get('gas','user_id')
RESULTS_BUCKET_NAME = config['s3']['ResultsBucketName']
CNET_ID = config['DEFAULT']['CnetId'] 

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
    # update dynamodb
    # https://stackoverflow.com/questions/55256127/update-item-in-dynamodb
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
        print("DynamoDB update successful")
        return response
    except Exception as e:
        print(f"Failed to update DynamoDB: {str(e)}")
        raise

def upload_file_to_s3(file_path, bucket, object_name):
    s3_client = boto3.client('s3',region_name=AWS_REGION)
    try:
        s3_client.upload_file(file_path, bucket, object_name)
        print(f"File {file_path} uploaded to {bucket}/{object_name}")
    except (NoCredentialsError, PartialCredentialsError, ClientError) as e:
        print(f"Failed to upload {file_path}. Error: {str(e)}")
        raise

def delete_local_file(file_path):
    try:
        os.remove(file_path)
        print(f"Deleted local file {file_path}")
    except OSError as e:
        print(f"Error: {file_path} : {e.strerror}")

if __name__ == '__main__':
    if len(sys.argv) > 1:
        input_file_path = sys.argv[1].strip()
        with Timer():
            results_file = input_file_path.replace('.vcf', '.annot.vcf')
            log_file = input_file_path + '.count.log'

            driver.run(input_file_path, 'vcf')

        # S3 Configuration
        bucket_name = RESULTS_BUCKET_NAME
        cnet_id = CNET_ID
        user_prefix = USER_ID # user_id from  session
        unique_id = os.path.basename(input_file_path).split('~')[0]

        s3_results_key = f"{cnet_id}/{user_prefix}/{unique_id}/{os.path.basename(results_file)}"
        s3_log_key = f"{cnet_id}/{user_prefix}/{unique_id}/{os.path.basename(log_file)}"

        upload_file_to_s3(results_file, bucket_name, s3_results_key)
        upload_file_to_s3(log_file, bucket_name, s3_log_key)

        delete_local_file(results_file)
        delete_local_file(log_file)

        # Update DynamoDB
        update_data = {
            's3_results_bucket': bucket_name,
            's3_key_result_file': s3_results_key,
            's3_key_log_file': s3_log_key
        }
        update_dynamodb(unique_id, update_data)
    else:
        print("A valid .vcf file must be provided as input to this program.")
