import json
import boto3
import pika
import ssl
import uuid
import datetime
import os
import requests

# AWS S3 configuration
s3 = boto3.client('s3')
s3_bucket = os.environ['S3_BUCKET']

# DynamoDB configuration
dynamodb = boto3.resource('dynamodb')
submission_table = dynamodb.Table(os.environ['DYNAMODB_TABLE'])

# CloudAMQP configuration
rabbitmq_host = os.environ['RABBITMQ_HOST']
rabbitmq_port = int(os.environ['RABBITMQ_PORT'])
rabbitmq_virtual_host = os.environ['RABBITMQ_VIRTUAL_HOST']
rabbitmq_username = os.environ['RABBITMQ_USERNAME']
rabbitmq_password = os.environ['RABBITMQ_PASSWORD']
rabbitmq_queue = os.environ['RABBITMQ_QUEUE']

class Submission:
    def __init__(self, submission_id, assignment_id, student_id, grade_level, image_url, created_at):
        self.submission_id = submission_id
        self.assignment_id = assignment_id
        self.student_id = student_id
        self.grade_level = grade_level
        self.image_url = image_url
        self.created_at = created_at

def lambda_handler(event, context):
    if 'httpMethod' in event and 'path' in event:
        # if event['httpMethod'] == 'POST' and event['path'] == '/prod/api/v2/submissions':
        if event['httpMethod'] == 'POST':
            # print(f"method ==== {event['httpMethod']}")
            return submit_artwork(event)
    return {'statusCode': 404, 'body': json.dumps({'message': 'Not Found'})}

def submit_artwork(event):
    body = json.loads(event['body'])
    assignment_id = body.get('assignmentId')
    student_id = body.get('studentId')
    grade_level = body.get('gradeLevel')
    image_url = body.get('imageUrl')

    if not assignment_id or not student_id or not image_url:
        return {'statusCode': 400, 'body': json.dumps({'message': 'Missing required fields'})}

    # Download the image from the provided URL
    response = requests.get(image_url)
    if response.status_code != 200:
        return {'statusCode': 400, 'body': json.dumps({'message': 'Failed to download image'})}

    # Generate a unique filename for the image
    file_extension = os.path.splitext(image_url)[1]
    image_filename = f"{uuid.uuid4()}{file_extension}"

    # Upload the image to S3
    image_key = f"{assignment_id}/{student_id}/{image_filename}"
    s3.put_object(Bucket=s3_bucket, Key=image_key, Body=response.content)

    # Generate the image URL
    image_url = f"https://{s3_bucket}.s3.amazonaws.com/{image_key}"

    # Generate a unique submission ID
    submission_id = str(uuid.uuid4())

    # Get the current timestamp
    created_at = datetime.datetime.now().isoformat()

    # Create a new submission item in DynamoDB
    submission_item = {
        'submission_id': submission_id,
        'assignment_id': assignment_id,
        'student_id': student_id,
        'grade_level': grade_level,
        'image_url': image_url,
        'created_at': created_at
    }
    submission_table.put_item(Item=submission_item)

    # Publish submission details to RabbitMQ
    publish_submission(submission_item)

    return {'statusCode': 200, 'body': json.dumps({'submissionId': submission_id, 'message': 'Submission successful'})}

def publish_submission(submission_data):
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    ssl_options = pika.SSLOptions(context)

    credentials = pika.PlainCredentials(rabbitmq_username, rabbitmq_password)
    parameters = pika.ConnectionParameters(
        host=rabbitmq_host,
        port=rabbitmq_port,
        virtual_host=rabbitmq_virtual_host,
        credentials=credentials,
        ssl_options=ssl_options
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=rabbitmq_queue)
    channel.basic_publish(exchange='', routing_key=rabbitmq_queue, body=json.dumps(submission_data))
    connection.close()
