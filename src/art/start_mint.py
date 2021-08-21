import boto3
from util import sudocoins_logger
from datetime import datetime
import uuid
import json

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')



def lambda_handler(event, context):
    body = json.loads(event['body'])

    time_now = str(datetime.utcnow().isoformat())
    art_id = str(uuid.uuid1())
    art_record = {
        'art_id': art_id,
        "name": body['name'],
        'buy_url': "",
        'contractId#tokenId': "",
        'preview_url': "",
        'art_url': "",
        "timestamp": time_now,
        "recent_sk": time_now + "#" + art_id,
        "click_count": 0,
        "first_user": body['user_id'],
        "sort_idx": 'true',
        "creator": body['public_address'],
        "process_status": "STREAM_TO_S3",
        "event_date": "0",
        "event_type": time_now,
        "blockchain": "Ethereum",
        "last_sale_price": 0
    }

    dynamodb.Table('art').put_item(Item=art_record)
    log.info("art record submitted")

    response = create_presigned_url("minting_bucket", art_id, expiration=360)
    log.info("pre-signed url retrieved")

    return response


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def create_presigned_url(bucket_name, object_name, expiration=360):
    # Generate a pre-signed URL for the S3 object
    s3_client = boto3.client('s3')
    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': object_name},
                                                    ExpiresIn=expiration)
    except Exception as e:
        log.info(e)
        return None

    # The response contains the pre-signed URL
    return response
