import boto3
import json
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    body = json.loads(event['body'])
    arts = body['arts']
    if len(arts) == 0:
        return {'arts': None}

    art_uploads_record = get_arts(arts)

    return {
        'arts': art_uploads_record
    }


def get_arts(art_ids):
    # returns art records. Single or batch. Argument must be a list
    client = boto3.client('dynamodb')
    art_keys = []

    for i in art_ids:
        element = {'art_id': {'S': i}}

        art_keys.append(element)

    art_record = client.batch_get_item(
        RequestItems={
            'art': {
                'Keys': art_keys,
                'ExpressionAttributeNames': {
                    '#N': 'name'
                },
                'ProjectionExpression': 'art_id, click_count, art_url,'
                                        'recent_sk, preview_url, #N, file_type, size'
            }
        }
    )

    print(type(art_record['Responses']['art']))

    newlist = sorted(art_record['Responses']['art'], key=lambda k: int(k['click_count']['N']), reverse=True)

    return newlist
