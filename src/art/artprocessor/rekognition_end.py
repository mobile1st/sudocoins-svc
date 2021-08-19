import boto3
import json
from util import sudocoins_logger

log = sudocoins_logger.get()

dynamodb = boto3.resource('dynamodb')
rekognition = boto3.client('rekognition')
sns_client = boto3.client('sns')

art_processor_topic = 'arn:aws:sns:us-west-2:977566059069:ArtProcessor'
start_label_detection_topic = 'arn:aws:sns:us-west-2:977566059069:ArtProcessorStartLabelDetection'


def lambda_handler(event, context):
    log.debug(f'event {event}')
    sns = event['Records'][0]['Sns']
    topic = sns['TopicArn']
    message = json.loads(sns['Message'])
    art_id = None
    labels = None
    if topic == art_processor_topic:
        art_id = message['artId']
        labels = get_as_string_set(message['rekognitionResponse']['Labels'])
    else:
        if topic == start_label_detection_topic:
            if message['Status'] == 'SUCCEEDED':
                job_id = message['JobId']
                art_id = message['Video']['S3ObjectName'].split('.')[0]
                response = rekognition.get_label_detection(JobId=job_id)
                log.info(f'response: {response}')
                labels = get_as_string_set(response['Labels'], True)
            else:
                log.warning(f'Rekognition StartLabelDetection status: {message["Status"]}')
        else:
            log.warning(f'Unhandled event: {event}')

    art_item = save_labels(art_id, labels)
    log.info(art_item)


def save_labels(art_id, labels):
    art_table = dynamodb.Table('art')
    if not labels:
        return art_table.get_item(Key={'art_id': art_id})['Item']

    return art_table.update_item(
        Key={'art_id': art_id},
        UpdateExpression='ADD rekognition_labels :labels REMOVE process_status',
        ExpressionAttributeValues={':labels': labels},
        ReturnValues='ALL_NEW'
    )['Attributes']


def get_as_string_set(labels, video_response=False):
    if video_response:
        return set([label['Label']['Name'] for label in labels])
    return set([label['Name'] for label in labels])
