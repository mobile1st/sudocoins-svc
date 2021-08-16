import boto3
import json
import time
from art.artprocessor.art_document import ArtDocument
from util import sudocoins_logger
from time import sleep

log = sudocoins_logger.get()

dynamodb = boto3.resource('dynamodb')
rekognition = boto3.client('rekognition')
kendra = boto3.client('kendra')
sns_client = boto3.client('sns')

art_processor_topic = 'arn:aws:sns:us-west-2:977566059069:ArtProcessor'
start_label_detection_topic = 'arn:aws:sns:us-west-2:977566059069:ArtProcessorStartLabelDetection'
kendra_index_id = '8f96a3bb-3aae-476e-94ec-0d446877b42a'
kendra_data_source_id = '52596114-645e-40fa-b154-3ada7b3a7942'


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
    push_art_to_kendra(art_item)


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


def push_art_to_kendra(art_item):
    art_id = art_item['art_id']
    data = art_item.get('open_sea_data', {})
    art_doc = ArtDocument() \
        .art_id(art_id) \
        .name(art_item.get('name')) \
        .description(data.get('description')) \
        .user_provided_tags(art_item.get('tags')) \
        .rekognition_labels(art_item.get('rekognition_labels'))
    log.info(f'document: {art_doc}')
    put_document(art_doc)


def put_document(art_doc):
    job_execution_id = get_job_execution_id()
    try:
        put_response = kendra.batch_put_document(
            IndexId=kendra_index_id,
            Documents=[art_doc.to_kendra_doc(kendra_data_source_id, job_execution_id)]
        )
        log.debug(f'job_execution_id: {job_execution_id} batch_put_document response: {put_response}')
    finally:
        kendra.stop_data_source_sync_job(
            Id=kendra_data_source_id,
            IndexId=kendra_index_id
        )


def get_job_execution_id():
    kendra.stop_data_source_sync_job(
        Id=kendra_data_source_id,
        IndexId=kendra_index_id
    )
    i = 0
    start = time.time()
    while True:
        i += 1
        job_execution_id = acquire_job_execution_id()
        if job_execution_id:
            break
        if time.time() - start > 5.0:
            log.warning(f'could not acquire job_execution_id after {i} tries')
            raise Exception('could not acquire job_execution_id')
        sleep(0.1)
    if i > 1:
        log.info(f'job_execution_id acquired after {i} tries')
    return job_execution_id


def acquire_job_execution_id():
    try:
        return kendra.start_data_source_sync_job(
            Id=kendra_data_source_id,
            IndexId=kendra_index_id
        )['ExecutionId']
    except kendra.exceptions.ConflictException:
        return None


def get_as_string_set(labels, video_response=False):
    if video_response:
        return set([label['Label']['Name'] for label in labels])
    return set([label['Name'] for label in labels])
