import boto3
import json
from util import sudocoins_logger
from art.art import Art

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    log.info(f'get_preview {event}')

    query_params = event['queryStringParameters']
    art_id = query_params.get('id')
    user_agent = event['user_agent']

    if user_agent.find('facebookexternalhit') > -1:
        art_object = get_by_share_id(art_id)
        return art_object

    elif user_agent.find('Twitterbot') > -1:
        art_object = get_by_share_id(art_id)
        return art_object

    else:
        return {
            "statusCode": 302,
            "headers": {'Location': ('https://www.sudocoins.com/art/' + art_id)},
            "body": json.dumps({})
        }


def get_by_share_id(share_id):
    # returns the art_uploads record based on shareId
    art_uploads_record = dynamodb.Table('art_uploads').get_item(
        Key={'shareId': share_id},
        ProjectionExpression="art_id, preview_url, art_url, #n, click_count",
        ExpressionAttributeNames={'#n': 'name'}
    )
    if 'Item' in art_uploads_record:
        return art_uploads_record['Item']

    art_record = dynamodb.Table('art').get_item(
        Key={'art_id': share_id},
        ProjectionExpression="art_id, preview_url, art_url, #n, click_count, buy_url",
        ExpressionAttributeNames={'#n': 'name'})
    if 'Item' in art_record:
        return art_record['Item']

    return {
        "message": "Art not found. Add generic preview data"
    }
