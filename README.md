# GAS Framework
An enhanced web framework (based on [Flask](https://flask.palletsprojects.com/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](https://getbootstrap.com/docs/3.3/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts/apps for notifications, archival, and restoration
* `/aws` - AWS user data files

Initiate restoration in the /subscribe endpoint by using an SNS thaw request topic to notify the system to initiate the thaw process. My thaw process uses a Flask app with a webhook approach and updates the job status to 'RESTORING' to enable the front-end to identify this status. The advantage of using SNS is that it can handle a large number of messages and distribute them to multiple endpoints, allowing for scalable processing. However, continuous polling using SNS and SQS can be better for debugging, as SQS can be used to check the required key from the thaw process.

The Flask approach highly depends on network stability and may not be secure. After the thawing is completed, the restoration process is handled by a Lambda function. This function tis a type of  serverless computing to perform the restoration job, scaling automatically based on the number of incoming requests. Lambda integrates seamlessly with other AWS services, facilitating an event-driven architecture. The Lambda function is triggered by an SNS restore topic, and upon completion, it updates the job status to 'RESTORED', allowing the front-end system to easily detect the job status.