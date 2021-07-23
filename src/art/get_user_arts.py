import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key
from art.art import Art

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
arts = Art(dynamodb)


def lambda_handler(event, context):
    # returns the art shared by the user
    set_log_context(event)
    user_id = event['pathParameters']['userId']
    return {
        'art': get_uploads(user_id)
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def get_uploads(user_id):
    # returns the user's uploaded art sorted by timestamp
    uploads = dynamodb.Table('art_uploads').query(
        KeyConditionExpression=Key('user_id').eq(user_id),
        ScanIndexForward=False,
        IndexName='User_uploaded_art_view_idx',
        ExpressionAttributeNames={'#n': 'name'},
        ProjectionExpression='shareId, click_count, art_url, art_id, preview_url, #n'

    )['Items']

    art_ids = [i['art_id'] for i in uploads]
    art_list = arts.get_arts(art_ids)
    art_index = {}
    for art in art_list:
        art_index[art['art_id']] = art

    for a in uploads:
        idx = art_index.get(a['art_id'])
        if idx and idx.get('art_url'):
            a['art_url'] = idx['art_url']  # this maybe a cdn url
        if idx and idx.get('mime_type'):
            a['mime_type'] = idx.get('mime_type')

    return uploads
