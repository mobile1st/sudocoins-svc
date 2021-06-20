import boto3
import json
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key
import uuid

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sqs = boto3.resource('sqs')


def lambda_handler(event, context):
    log.info(f'art_prompt {event}')

    share_id = event['rawPath'].replace('/art/share/', '')
    source_ip = event['requestContext']['http']['sourceIp']
    try:
        query_params = event['queryStringParameters']
        unique_id = query_params.get('userId')
        if unique_id:
            print("true")
            user_id = unique_id
        else:
            user_id = source_ip
    except Exception as e:
        user_id = source_ip


    return get_by_share_id(source_ip, share_id, user_id)


def get_by_share_id(source_ip, share_id, user_id):
    # returns the art_uploads record based on shareId
    art_uploads_record = dynamodb.Table('art_uploads').get_item(
        Key={'shareId': share_id},
        ProjectionExpression="art_id, preview_url, art_url, #n, click_count",
        ExpressionAttributeNames={'#n': 'name'}
    )

    print(art_uploads_record)

    queue = sqs.get_queue_by_name(QueueName='ArtViewCounterQueue.fifo')
    # queue deduplication by sourceIp+artId/shareId for 5 minutes
    msg = {'sourceIp': source_ip}
    if 'Item' in art_uploads_record:
        msg['shareId'] = share_id
        log.debug(f'sending message: {msg}')
        queue.send_message(MessageBody=json.dumps(msg), MessageGroupId='share_views')
        next_art = get_next_preview(user_id)

        return {
            "art_id": art_uploads_record['Item']['art_id'],
            "click_count": art_uploads_record['Item']['click_count'],
            "name": art_uploads_record['Item']['click_count'],
            "art_url": art_uploads_record['Item']['art_url'],
            "preview_url": art_uploads_record['Item']['preview_url'],
            "prompt": art_uploads_record['Item'],
            "next": next_art
            }

    art_record = dynamodb.Table('art').get_item(
        Key={'art_id': share_id},
        ProjectionExpression="art_id, preview_url, art_url, #n, click_count, buy_url",
        ExpressionAttributeNames={'#n': 'name'})

    if 'Item' in art_record:
        msg['art_id'] = share_id
        log.debug(f'sending message: {msg}')
        queue.send_message(MessageBody=json.dumps(msg), MessageGroupId='share_views')
        next_art = get_next_preview(user_id)

        return {
            "art_id": art_record['Item']['art_id'],
            "click_count": art_record['Item']['click_count'],
            "name": art_record['Item']['click_count'],
            "art_url": art_record['Item']['art_url'],
            "preview_url": art_record['Item']['preview_url'],
            "prompt": art_record['Item'],
            "next": next_art
            }

    return


def get_next_preview(user_id):
    art_votes = get_votes(user_id)
    recent_arts = get_recent(20, str(uuid.uuid1()))
    if len(art_votes) == 0:
        return {
            "art_id": recent_arts[0]['art_id'],
            "art_url": recent_arts[0]['art_url'],
            "preview_url": recent_arts[0]['preview_url'],
            "recent_sk": recent_arts[0]['recent_sk']
        }
    count = 20
    while count > 0:
        for i in recent_arts:
            for k in art_votes:
                if i['art_id'] == k['art_id']:
                    continue
                else:
                    return {
                        "art_id": i['art_id'],
                        "art_url": i['art_url'],
                        "preview_url": i['preview_url'],
                        "recent_sk": i['recent_sk']
                    }
            count -= 1
            if count == 0:
                recent_arts = get_recent(20, i['recent_sk'])
                count = len(recent_arts)

    return


def get_recent(count, timestamp):
    return dynamodb.Table('art').query(
        KeyConditionExpression=Key("sort_idx").eq('true') & Key("recent_sk").lt(timestamp),
        ScanIndexForward=False,
        Limit=count,
        IndexName='Recent_index',
        ProjectionExpression="art_id, preview_url, art_url, #n, click_count, recent_sk",
        ExpressionAttributeNames={'#n': 'name'}
    )['Items']


def get_votes(unique_id):
    return dynamodb.Table('art_votes').query(
        KeyConditionExpression=Key("unique_id").eq(unique_id)
    )['Items']