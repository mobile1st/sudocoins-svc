import boto3
from util import sudocoins_logger
from art.art import Art
from boto3.dynamodb.conditions import Key

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
arts = Art(dynamodb)


def lambda_handler(event, context):
    set_log_context(event)
    query_params = event['queryStringParameters']
    user_id = query_params.get('user_id')
    log.info(f"user_id {user_id}")

    return {
        'art': get_hearts(user_id)
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def get_hearts(user_id):
    res = dynamodb.Table('art_votes').query(
        KeyConditionExpression=Key("user_id").eq(user_id) & Key("type").eq("vote"),
        IndexName='user_id-type-index',
        ProjectionExpression="art_id"
    )
    if res['Count'] > 0:
        return res['Items']
    else:
        return []


