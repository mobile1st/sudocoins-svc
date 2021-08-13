import boto3
from art.art import Art
from util import sudocoins_logger
import uuid
from datetime import datetime
from boto3.dynamodb.conditions import Key

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
arts = Art(dynamodb)


def lambda_handler(event, context):
    set_log_context(event)
    log.debug(f'set_vote event{event}')
    query_params = event['queryStringParameters']

    if query_params['vote'] == 1:
        dynamodb.Table('art').update_item(
            Key={'art_id': query_params.get('artId')},
            UpdateExpression="SET hearts = if_not_exists(hearts, :start) + :inc",
            ExpressionAttributeValues={
                ':inc': 1,
                ':start': 0
            }
        )
        art_votes_record = {
            "unique_id": str(uuid.uuid1()),
            "user_id": query_params.get('id'),
            "art_id": query_params.get('artId'),
            "ip": query_params.get('ip', ""),
            "vote": 1,
            "timestamp": str(datetime.utcnow().isoformat()),
            "type": "vote"
        }
        dynamodb.Table('art_votes').put_item(
            Item=art_votes_record
        )
        log.info("heart added")

    elif query_params['vote'] == 0:
        dynamodb.Table('art').update_item(
            Key={'art_id': query_params.get('artId')},
            UpdateExpression="SET hearts = if_not_exists(hearts, :start) - :inc",
            ExpressionAttributeValues={
                ':inc': 1,
                ':start': 0
            },
            ReturnValues="UPDATED_NEW"
        )
        res = dynamodb.Table('art_votes').query(
            KeyConditionExpression=Key("user_id").eq(query_params.get('id')) & Key("art_id").eq(query_params.get('artId')),
            IndexName='user_id-art_id-index',
            ProjectionExpression="unique_id"
        )
        log.info(f'query response: {res}')
        if res['Count'] > 0:
            unique_id = ['Items'][0]['unique_id']
            dynamodb.Table('art_votes').delete_item(
                Key={
                    'unique_id': unique_id
                })
    return


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


'''
    code for next newest art (old logic)
    
    art_votes = get_votes(user_id)
    recent_arts = arts.get_recent(20, timestamp)
    if len(art_votes) == 0:
        return {
            "art_id": recent_arts[0]['art_id'],
            "art_url": recent_arts[0]['art_url'],
            "preview_url": recent_arts[0]['preview_url'],
            "recent_sk": recent_arts[0]['recent_sk']
        }

    votes_list = []
    for i in art_votes:
        votes_list.append(i['art_id'])

    count = 20
    while count > 0:
        for k in recent_arts:
            if k['art_id'] in votes_list:
                count -= 1
                if count == 0:
                    recent_arts = arts.get_recent(20, k['recent_sk'])
                    count = len(recent_arts)
            else:
                return {
                    "art_id": k['art_id'],
                    "art_url": k['art_url'],
                    "preview_url": k['preview_url'],
                    "recent_sk": k['recent_sk']
                }

    return


def get_votes(user_id):
    return dynamodb.Table('art_votes').query(
        KeyConditionExpression=Key("user_id").eq(user_id),
        IndexName='user_id-index'
    )['Items']
'''





