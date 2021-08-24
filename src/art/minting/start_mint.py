import boto3
from util import sudocoins_logger
from datetime import datetime
import uuid
import json

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    art_id = str(uuid.uuid1())
    body = json.loads(event['body'])
    file_ext = body['file_ext']
    file_name = art_id + "." + file_ext

    response = create_presigned_url("sudocoins-art-bucket", file_name, expiration=360)
    log.info("pre-signed url retrieved")

    return {
        "file_name": file_name,
        "presigned_url": response
    }


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
