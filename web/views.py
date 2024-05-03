# views.py
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import uuid
import time
import json
import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from datetime import datetime, timezone
# Import decimal for handling DynamoDB's decimal 
#https://stackoverflow.com/questions/70343666/python-boto3-float-types-are-not-supported-use-decimal-types-instead
import decimal

from flask import abort, flash, redirect, render_template, request, session, url_for, jsonify, request
# Import ZoneInfo from zoneinfo module
# https://www.educative.io/answers/what-is-zoneinfozoneinfokey-in-python
from zoneinfo import ZoneInfo 
from app import app, db
from decorators import authenticated, is_premium

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""


@app.route("/annotate", methods=["GET"])
@authenticated
def annotate():
    s3 = boto3.client(
        "s3",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version="s3v4"),
    )

    user_id = session["primary_identity"]
    key_name = f"{app.config['AWS_S3_KEY_PREFIX']}{user_id}/{str(uuid.uuid4())}~${{filename}}"

    redirect_url = request.url + "/job"

    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": app.config["AWS_S3_ENCRYPTION"],
        "acl": app.config["AWS_S3_ACL"],
        "csrf_token": app.config["SECRET_KEY"],
    }
    conditions = [
        ["starts-with", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": app.config["AWS_S3_ENCRYPTION"]},
        {"acl": app.config["AWS_S3_ACL"]},
        ["starts-with", "$csrf_token", ""],
    ]

    try:
        presigned_post = s3.generate_presigned_post(
            Bucket=app.config["AWS_S3_INPUTS_BUCKET"],
            Key=key_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=app.config["AWS_SIGNED_REQUEST_EXPIRATION"],
        )
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        # use abort in python
        # https://www.w3schools.com/python/ref_os_abort.asp#:
        return abort(500)  # Error page for server error

    return render_template("annotate.html", s3_post=presigned_post, role=session["role"])


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""


@app.route("/annotate/job", methods=["GET"])
@authenticated
def create_annotation_job_request():
    region = app.config["AWS_REGION_NAME"]
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])

    bucket_name = request.args.get("bucket")
    s3_key = request.args.get("key")
    last_part = s3_key.split('/')[-1]
    job_id = last_part.split('~')[0]
    input_filename = last_part.split('~')[-1]
    submit_time = int(datetime.now(timezone.utc).timestamp())
    user_id = session["primary_identity"]

    data = {
        "job_id": job_id,
        "user_id": user_id,
        "input_file_name": input_filename,
        "s3_inputs_bucket": bucket_name,
        "s3_key_input_file": s3_key,
        "submit_time": submit_time,
        "job_status": "PENDING"
    }

    try:
        table.put_item(Item=data)
    except ClientError as e:
        app.logger.error(f"Error writing to DynamoDB: {e}")
        return abort(500)  # Server error page if DynamoDB write fails

    try:
        sns_client = boto3.client('sns', region_name=region)
        sns_client.publish(
            TopicArn=app.config["AWS_SNS_JOB_REQUEST_TOPIC"],
            Message=json.dumps({'default': json.dumps(data)}),
            MessageStructure='json'
        )
    except ClientError as e:
        app.logger.error(f"Error posting request to SNS: {e}")
        return abort(500)  # Server error page if SNS post fails

    return render_template("annotate_confirm.html", job_id=job_id)


"""List all annotations for the user
"""


@app.route("/annotations", methods=["GET"])
@authenticated
def annotations_list():
    user_id = session.get("primary_identity")
    if not user_id:
        abort(403)  # Forbidden access if not authenticated

    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])
    
    # Convert UTC to CST using ZoneInfo
    # https://community-forums.domo.com/main/discussion/53456/convert-timestamp-to-cst-ust-mst-est-python
    cst = ZoneInfo('America/Chicago')

    try:
        response = table.query(
            IndexName='user_id_index',  # The name of the secondary index
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
    except ClientError as e:
        app.logger.error(f"Error fetching annotations from DynamoDB: {e}")
        abort(500)  # Internal server error

    jobs = response.get('Items', [])
    # Format the date/time and adjust to CST
    for job in jobs:
        # Convert Decimal to int for timestamp
        if 'submit_time' in job:
            utc_timestamp = int(job['submit_time'])
            utc_time = datetime.fromtimestamp(utc_timestamp, tz=ZoneInfo('UTC'))
            cst_time = utc_time.astimezone(cst)
            job['submit_time'] = cst_time.strftime('%Y-%m-%d %H:%M:%S')

    # Check if there are no jobs found
    if not jobs:
        flash("No annotations found", "info")

    return render_template("annotations.html", annotations=jobs)

def generate_presigned_url(bucket_name, object_name, expiration=3600):
    """Generate a presigned URL to share an S3 object"""
    s3_client = boto3.client(
        "s3",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version="s3v4"),
    )
    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': object_name},
                                                    ExpiresIn=expiration)
    except ClientError as e:
        app.logger.error(f"Couldn't generate presigned URL: {e}")
        abort(500)
    return response

"""Display details of a specific annotation job
"""

@app.route("/annotations/<id>", methods=["GET"])
@authenticated
def annotation_details(id):
    user_id = session.get("primary_identity")
    if not user_id:
        abort(403)  # User is not authenticated

    dynamodb = boto3.resource('dynamodb', region_name=app.config["AWS_REGION_NAME"])
    table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])

    try:
        response = table.get_item(
            Key={'job_id': id}
        )
        job = response.get('Item', None)
        if not job or job['user_id'] != user_id:
            abort(403)  # Unauthorized access to a job not owned by the user
        
        # Generate download URLs for input and result files
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
        input_file_url = generate_presigned_url(app.config['AWS_S3_INPUTS_BUCKET'], job['s3_key_input_file'])
        results_file_url = generate_presigned_url(app.config['AWS_S3_RESULTS_BUCKET'], job['s3_key_result_file']) if job['job_status'] == 'COMPLETED' else None
        
        # Convert times to human-readable CST format
        cst_zone = ZoneInfo('America/Chicago')
        if 'submit_time' in job:
            submit_time = int(job['submit_time'])  # Convert Decimal to int
            job['submit_time'] = datetime.fromtimestamp(submit_time).astimezone(cst_zone).strftime('%Y-%m-%d %H:%M:%S')

        if 'complete_time' in job and job['complete_time'] is not None:
            complete_time = int(job['complete_time'])  # Convert Decimal to int
            job['complete_time'] = datetime.fromtimestamp(complete_time).astimezone(cst_zone).strftime('%Y-%m-%d %H:%M:%S')

    except ClientError as e:
        app.logger.error(f"Error fetching job from DynamoDB: {e}")
        abort(500)  # Internal Server Error
    

    return render_template("annotation.html", job=job, input_file_url=input_file_url, results_file_url=results_file_url)  # Render HTML with job details

"""Display the log file contents for an annotation job
"""


@app.route("/annotations/<id>/log", methods=["GET"])
@authenticated
def annotation_log(id):
    user_id = session.get("primary_identity")
    if not user_id:
        abort(403)  # Unauthorized access

    dynamodb = boto3.resource('dynamodb', region_name=app.config["AWS_REGION_NAME"])
    table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])

    try:
        response = table.get_item(Key={'job_id': id})
        job = response.get('Item')
        if not job or job['user_id'] != user_id:
            abort(403)  # Unauthorized access

        # Retrieve the log file from S3
        s3_client = boto3.client('s3')
        log_file_key = job.get('s3_key_log_file')
        obj = s3_client.get_object(Bucket=app.config['AWS_S3_RESULTS_BUCKET'], Key=log_file_key)
        log_content = obj['Body'].read().decode('utf-8')  # Decode the content to a string
        # https://www.tutorialspoint.com/python/string_decode.htm
        
    except ClientError as e:
        app.logger.error(f"Error fetching log file: {e}")
        abort(500)  # Internal Server Error

    return render_template("view_log.html", job_id=id, log_content=log_content)

"""Subscription management handler
"""
import stripe
from auth import update_profile


@app.route("/subscribe", methods=["GET", "POST"])
def subscribe():
    if request.method == "GET":
        # Display form to get subscriber credit card info

        # If A15 not completed, force-upgrade user role and initiate restoration
        pass

    elif request.method == "POST":
        # Process the subscription request

        # Create a customer on Stripe

        # Subscribe customer to pricing plan

        # Update user role in accounts database

        # Update role in the session

        # Request restoration of the user's data from Glacier
        # ...add code here to initiate restoration of archived user data
        # ...and make sure you handle files pending archive!

        # Display confirmation page
        pass


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Set premium_user role
"""


@app.route("/make-me-premium", methods=["GET"])
@authenticated
def make_me_premium():
    # Hacky way to set the user's role to a premium user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="premium_user")
    return redirect(url_for("profile"))


"""Reset subscription
"""


@app.route("/unsubscribe", methods=["GET"])
@authenticated
def unsubscribe():
    # Hacky way to reset the user's role to a free user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="free_user")
    return redirect(url_for("profile"))


"""Home page
"""


@app.route("/", methods=["GET"])
def home():
    return render_template("home.html"), 200


"""Login page; send user to Globus Auth
"""


@app.route("/login", methods=["GET"])
def login():
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    # If user requested a specific page, save it session for redirect after auth
    if request.args.get("next"):
        session["next"] = request.args.get("next")
    return redirect(url_for("authcallback"))


"""404 error handler
"""


@app.errorhandler(404)
def page_not_found(e):
    return (
        render_template(
            "error.html",
            title="Page not found",
            alert_level="warning",
            message="The page you tried to reach does not exist. \
      Please check the URL and try again.",
        ),
        404,
    )


"""403 error handler
"""


@app.errorhandler(403)
def forbidden(e):
    return (
        render_template(
            "error.html",
            title="Not authorized",
            alert_level="danger",
            message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party.",
        ),
        403,
    )


"""405 error handler
"""


@app.errorhandler(405)
def not_allowed(e):
    return (
        render_template(
            "error.html",
            title="Not allowed",
            alert_level="warning",
            message="You attempted an operation that's not allowed; \
      get your act together, hacker!",
        ),
        405,
    )


"""500 error handler
"""


@app.errorhandler(500)
def internal_error(error):
    return (
        render_template(
            "error.html",
            title="Server error",
            alert_level="danger",
            message="The server encountered an error and could \
      not process your request.",
        ),
        500,
    )


"""CSRF error handler
"""


from flask_wtf.csrf import CSRFError


@app.errorhandler(CSRFError)
def csrf_error(error):
    return (
        render_template(
            "error.html",
            title="CSRF error",
            alert_level="danger",
            message=f"Cross-Site Request Forgery error detected: {error.description}",
        ),
        400,
    )


### EOF