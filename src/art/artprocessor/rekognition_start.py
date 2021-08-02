import boto3
import json
from util import sudocoins_logger

log = sudocoins_logger.get()

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
        if mime_type and cdn_url:
            art_s3_name = cdn_url.replace(cdn_url_prefix, '')
            if 'image/jpeg' in mime_type or 'image/png' in mime_type:
                response = call_image_rekognition(cdn_url.replace(cdn_url_prefix, ''))
                if response:
                    send_notification(art_id, response)
            else:
                if 'video/mp4' in mime_type or 'video/quicktime' in mime_type:
                    call_video_rekognition(art_s3_name)

        log.info(f'processed: {art_id}')
    except Exception as e:
        log.exception(f'{art_id} exception: {e}')


def send_notification(art_id, rekognition_response):
    process_status = 'REKOGNITION_END'
    sns_client.publish(
        TopicArn='arn:aws:sns:us-west-2:977566059069:ArtProcessor',
        MessageStructure='string',
        MessageAttributes={
            'art_id': {
                'DataType': 'String',
                'StringValue': art_id
            },
            'process': {
                'DataType': 'String',
                'StringValue': process_status
            }
        },
        Message=json.dumps({
            'artId': art_id,
            'rekognitionResponse': rekognition_response
        })
    )
    log.info(f'{art_id} published process status: {process_status}')


def call_image_rekognition(art_s3_name):
    try:
        return rekognition.detect_labels(
            Image={
                'S3Object': {
                    'Bucket': 'sudocoins-art-bucket',
                    'Name': art_s3_name
                }
            },
            MinConfidence=70
        )
    except Exception as e:
        log.warning(f'rekognition failed for {art_s3_name}, reason: {e}')
        return None


def call_video_rekognition(art_s3_name):
    rekognition.start_label_detection(
        Video={
            'S3Object': {
                'Bucket': 'sudocoins-art-bucket',
                'Name': art_s3_name
            }
        },
        MinConfidence=70,
        NotificationChannel={
            'SNSTopicArn': 'arn:aws:sns:us-west-2:977566059069:ArtProcessorStartLabelDetection',
            'RoleArn': 'arn:aws:iam::977566059069:role/SudocoinsStack-RekognitionArtProcessorStartLabelDe-1V1RMJ9IXQINJ'
        }
    )
