import boto3
from botocore.config import Config
import json
import sudocoins_logger

log = sudocoins_logger.get()
config = Config(connect_timeout=0.1, read_timeout=0.1, retries={'max_attempts': 5, 'mode': 'standard'})
dynamodb = boto3.resource('dynamodb', config=config)


def lambda_handler(event, context):
    log.debug(f'event: {event}')
    if event.get('queryStringParameters') is None:
        log.warn('the request does not contain query parameters')
        return {
            "statusCode": 302,
            "headers": {'Location': 'https://www.sudocoins.com/?msg=invalid'},
            "body": json.dumps({})
        }
    params = event['queryStringParameters']
    row_id = params["id"]

    art_uploads_table = dynamodb.Table('art_uploads')
    art_uploads_table.update_item(
        Key={'shareId': row_id},
        UpdateExpression="SET click_count = if_not_exists(click_count , :start) + :inc",
        ExpressionAttributeValues={
            ':inc': 1,
            ':start': 0
        },
        ReturnValues="UPDATED_NEW"
    )

    row = art_uploads_table.get_item(Key={'shareId': row_id})
    print(row)

    redirect_url = row['Item']['url']
    art_id = row['Item']['art_id']

    art_table = dynamodb.Table('art')
    art_table.update_item(
        Key={'art_id': art_id},
        UpdateExpression="SET click_count = if_not_exists(click_count , :start) + :inc",
        ExpressionAttributeValues={
            ':inc': 1,
            ':start': 0
        },
        ReturnValues="UPDATED_NEW"
    )



    response = {
        "statusCode": 302,
        "headers": {'Location': redirect_url},
        "body": json.dumps({})
    }

    return response

