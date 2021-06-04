import boto3
import json
import uuid
from datetime import datetime
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    log.debug(f'event: {event}')
    try:
        body = json.loads(event['body'])
        user_id = body['userId']
        art_id = body['art_id']
        log.info(f'artId: {art_id}')
        return share(user_id, art_id)
    except Exception as e:
        log.exception(e)


def share(user_id, art_id):
    time_now = str(datetime.utcnow().isoformat())

    art_record = dynamodb.Table('art').get_item(
        Key={'art_id': art_id},
        ProjectionExpression="art_id, preview_url, art_url, #n, click_count, buy_url, #tc,"
                             "open_sea_data ",
        ExpressionAttributeNames={'#n': 'name', '#tc': 'contractId#tokenId'})

    if 'Item' in art_record:
        dedupe_key = str(user_id) + '#' + art_record['Item']['contractId#tokenId']
        art_uploads_Object = dynamodb.Table('art_uploads').query(
            KeyConditionExpression=Key("dedupe_key").eq(dedupe_key),
            IndexName='User_upload_dedupe_idx')
        if art_uploads_Object['Count'] > 0:
            msg = {
                "shareId": art_uploads_Object['Items'][0]['shareId']
            }
            return msg
        else:
            shareId = str(uuid.uuid1())
            art_uploads_record = {
                "shareId": shareId,
                'contractId#tokenId': art_record['Item']['contractId#tokenId'],
                "name": art_record['Item']['name'],
                "buy_url": art_record['Item']['buy_url'],
                "user_id": user_id,
                'preview_url': art_record['Item']['preview_url'],
                'art_url': art_record['Item']['art_url'],
                "open_sea_data": art_record['Item']['open_sea_data'],
                "click_count": 0,
                "timestamp": time_now,
                "dedupe_key": dedupe_key,
                "art_id": art_id
            }
            dynamodb.Table('art_uploads').put_item(
                Item=art_uploads_record
            )
            msg = {
                "shareId": shareId
            }
            return msg
    else:
        art_uploads_record = dynamodb.Table('art_uploads').get_item(
            Key={'shareId': art_id},
            ProjectionExpression="art_id, user_id, shareId, #n,"
                                 "buy_url, preview_url, art_url, open_sea_data, #tc ",
            ExpressionAttributeNames={'#n': 'name', '#tc': 'contractId#tokenId'})['Item']
        if user_id == art_uploads_record['user_id']:
            msg = {
                "shareId": art_uploads_record['shareId']
            }
            return msg
        else:
            shareId = str(uuid.uuid1())
            art_uploads_record = {
                "shareId": shareId,
                'contractId#tokenId': art_uploads_record['contractId#tokenId'],
                "name": art_uploads_record['name'],
                "buy_url": art_uploads_record['buy_url'],
                "user_id": user_id,
                'preview_url': art_uploads_record['preview_url'],
                'art_url': art_uploads_record['art_url'],
                "open_sea_data": art_uploads_record['open_sea_data'],
                "click_count": 0,
                "timestamp": time_now,
                "dedupe_key": str(user_id) + '#' + art_uploads_record['contractId#tokenId'],
                "art_id": art_id
            }
            dynamodb.Table('art_uploads').put_item(
                Item=art_uploads_record
            )
            msg = {
                "shareId": shareId
            }
            return msg
