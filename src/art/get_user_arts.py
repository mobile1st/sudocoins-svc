import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    # returns the art shared by the user
    user_id = event['pathParameters']['userId']
    return {
        'art': get_uploads(user_id)
    }


def get_uploads(user_id):
    # returns the user's uploaded art sorted by timestamp
    return dynamodb.Table('art_uploads').query(
        KeyConditionExpression=Key('user_id').eq(user_id),
        ScanIndexForward=False,
        IndexName='User_uploaded_art_view_idx',
        ExpressionAttributeNames={'#n': 'name'},
        ProjectionExpression='shareId, click_count, art_url, art_id,'
                             'preview_url, #n'
    )['Items']
