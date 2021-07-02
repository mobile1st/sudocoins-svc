import boto3
import json
from util import sudocoins_logger
from art.art import Art
import requests

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    log.debug('art processor called')
    data = json.loads(event['Records'][0]['Sns']['Message'])
    log.info(f'payload: {data}')
    stream_to_s3(data)
    log.info('record updated')


def stream_to_s3(data):
    art_url = data['art_url']

    response = requests.get(art_url, stream=True)
    log.info(response.headers)
    file_type = response.headers['content-type']
    type_index = file_type.find('/')
    file_ending = file_type[type_index + 1:]

    s3_bucket = "artprocessor"
    s3_file_path = data['art_id'] + '.' + file_ending
    response.raw.decode_content = True
    conf = boto3.s3.transfer.TransferConfig(multipart_threshold=10000, max_concurrency=4)
    client = boto3.client('s3')
    client.upload_fileobj(response.raw, s3_bucket, s3_file_path, Config=conf)
    # client.put_object(Body=response, Bucket='artprocessor', Key=s3_file_path)
    log.info('upload finished')

    art_table = dynamodb.Table('art')
    art_table.update_item(
        Key={'art_id': data['art_id']},
        UpdateExpression="SET headers=:head",
        ExpressionAttributeValues={
            ':head': response.headers
        },
        ReturnValues="UPDATED_NEW"
    )
    log.info("art headers added to art table")

    return
