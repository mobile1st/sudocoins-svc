import boto3
import json
from util import sudocoins_logger

log = sudocoins_logger.get()

dynamodb = boto3.resource('dynamodb')
rekognition = boto3.client('rekognition')
sns_client = boto3.client('sns')
cdn_url_prefix = 'https://cdn.sudocoins.com/'
art_processor_topic = 'arn:aws:sns:us-west-2:977566059069:ArtProcessor'
start_label_detection_topic = 'arn:aws:sns:us-west-2:977566059069:ArtProcessorStartLabelDetection'


def lambda_handler(event, context):
    sns = event['Records'][0]['Sns']
    topic = sns['TopicArn']
    message = json.loads(sns['Message'])
    if topic == art_processor_topic:
        handle_image_labels(message['artId'], message['rekognitionResponse'])
    else:
        if topic == start_label_detection_topic:
            if message['Status'] == 'SUCCEEDED':
                job_id = message['JobId']
                art_id = message['Video']['S3ObjectName'].split('.')[0]
                handle_video_labels(art_id, job_id)
            else:
                log.warning(f'Rekognition StartLabelDetection status: {message["Status"]}')
        else:
            log.warning(f'Unhandled event: {event}')


def handle_image_labels(art_id, rekognition_response):
    log.info(f'art_id: {art_id}, rekognition_response: {rekognition_response}')


def handle_video_labels(art_id, job_id):
    log.info(f'art_id: {art_id}, job_id: {job_id}')
