import boto3
import json
import sys
import os
from configparser import ConfigParser, ExtendedInterpolation
from botocore.exceptions import ClientError

# Add parent directory to path to import helpers
sys.path.insert(1, os.path.join(sys.path[0], '..'))
import helpers

# Load configuration
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read('../util_config.ini')
config.read('notify_config.ini')

def send_notification_email(job):
    """Send an email notification about the completion of a job."""
    user_profile = helpers.get_user_profile(job['user_id'])
    # If user_profile is a list and email is the third element
    if isinstance(user_profile, list):
        user_email = user_profile[2]  # 'Email' is the third element in the list
    else:
        print("Unexpected user_profile format")
        return  # Return or handle error appropriately

    sender = config['ses']['SenderEmail']
    recipient = user_email
    subject = f"Results available for job {job['job_id']}"
    body = (
        f"Your annotation job completed at {job['complete_time']}. "
        f"Click here to view job details and results: "
        f"https://zhoua-a12-web.ucmpcs.org:4433/annotations/{job['job_id']}"
    )

    try:
        # Adjusting the call to match the helper function signature
        helpers.send_email_ses(recipients=recipient, sender=sender, subject=subject, body=body)
        print("Email sent successfully!")
    except ClientError as e:
        print(f"Failed to send email: {e}")


def handle_results_queue(sqs_client):
    """Process messages from the results queue."""
    queue_url = config['sqs']['JobResultsQueueUrl']
    max_messages = int(config['sqs']['MaxMessages'])
    wait_time = int(config['sqs']['WaitTime'])

    while True:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
            MessageAttributeNames=['All'],
            AttributeNames=['All']
        )
        messages = response.get('Messages', [])
        for message in messages:
            try:
                sns_message = json.loads(message['Body'])  # Parse the message body to get the SNS message
                job_details_json = sns_message['Message']  # Extract the actual job details from the 'Message' key
                job = json.loads(job_details_json)  # Parse the JSON string to get the job dictionary
                
                send_notification_email(job)
                
                sqs_client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
            except KeyError as e:
                print(f"Missing key {e} in message.")
            except Exception as e:
                print(f"Failed to process message: {e}")


def main():
    """Main function to set up AWS resources and poll the queue."""
    sqs_client = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])
    print("Starting notification service...")
    handle_results_queue(sqs_client)

if __name__ == "__main__":
    main()
