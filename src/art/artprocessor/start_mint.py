import boto3
from util import sudocoins_logger


log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')



def lambda_handler(event, context):

    return


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def create_presigned_url(bucket_name, object_name, expiration=360):
    # Generate a presigned URL for the S3 object
    s3_client = boto3.client('s3')
    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': object_name},
                                                    ExpiresIn=expiration)
    except Exception as e:
        log.info(e)
        return None

    # The response contains the presigned URL
    return response
