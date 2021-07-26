import boto3
import json
from util import sudocoins_logger

log = sudocoins_logger.get()

dynamodb = boto3.resource('dynamodb')
rekognition = boto3.client('rekognition')
sns_client = boto3.client('sns')
cdn_url_prefix = 'https://cdn.sudocoins.com/'


def lambda_handler(event, context):
    art = json.loads(event['Records'][0]['Sns']['Message'])
    log.info(f'payload: {art}')
    art_id = art['art_id']
    try:
        mime_type = art.get('mime_type')
        cdn_url = art.get('cdn_url')
        labels = None
        if mime_type and cdn_url and (mime_type == 'image/jpeg' or mime_type == 'image/png'):
            labels = get_rekognition_labels(cdn_url.replace(cdn_url_prefix, ''))
        log.info(f'{art_id}: {labels}')
        if labels:
            dynamodb.Table('art').update_item(
                Key={'art_id': art_id},
                UpdateExpression="ADD rekognition_labels :labels REMOVE process_status",
                ExpressionAttributeValues={":labels": labels},
            )
    except Exception as e:
        log.exception(f'{art_id} exception: {e}')


def get_rekognition_labels(art_s3_name):
    try:
        response = rekognition.detect_labels(
            Image={
                'S3Object': {
                    'Bucket': 'sudocoins-art-bucket',
                    'Name': art_s3_name
                }
            },
            MinConfidence=70
        )
        return get_as_string_set(response['Labels'])
    except Exception as e:
        log.warning(f'rekognition failed for {art_s3_name}, reason: {e}')
        return None


def get_as_string_set(labels):
    return set([label['Name'] for label in labels])
