# GAS Framework
An enhanced web framework (based on [Flask](https://flask.palletsprojects.com/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](https://getbootstrap.com/docs/3.3/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts/apps for notifications, archival, and restoration
* `/aws` - AWS user data files

I am using Flas app with an SNS-Subscribed Endpoint approach to complete my archive utility. The most important advantange of this approach is its even-driven architecture. This Flask app is processing messages as they come in through SNS notification. This is resource-efficient. I am expecting high volumes of archival requests where immediacy and real-time processing are crucial so that I choose Flask app approach.


In terms of timely archive, I chhose to use AWS step function. The first reason is that this step function allows me to manage the sequence of conditions of state transitions without needing to handle the wait logic within my application code which sinmplifes workflows. One important feature of Step function is stateless, they can scale automatically to handle increases in request without manual intervention which satifsy our goal: system needs to manage several of archival tasks simultaneously. 

Here is an overview of my state machine:
first state: AnnotationCompleted

Type: This state is a "Pass" type, which means it passes its input to its output without modification, effectively serving as a placeholder .
Next: This defines the next state to transition to after this state completes, which is "WaitState".
WaitState

Type: This is a "Wait" state, used to delay the state machine's execution.
Seconds: The duration of the wait specified in seconds. Here, it's set to 180 seconds. This state does not require any computational resources during the wait and allows time-based delay in the workflow.
Next: After the wait period, the state machine transitions to the "NotifySNS" state.
NotifySNS

Type: This is a "Task" type
Resource: Specifies the AWS service to use for this task. In this case, it's using the Amazon SNS service's "publish" functionality to send a message.
Parameters: These are the parameters passed to the SNS service.
TopicArn: The Amazon Resource Name (ARN) of the SNS topic to which the message will be published.
Message.$: Uses the state's entire input as the message to be published to the SNS topic. The "$" symbol denotes the use of the entire context data as the message.
End: Marks this state as the final state in the state machine. Once this state completes, the state machine execution is also complete.

After state machine(wait 3 minutes), it sends SNS notification which trigger archive process. 

Cons of my approach:
it relies on util server performance. 
According to my research, securing http endpoint is inmportant which I do not accomplish this

