import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
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

    artTable = dynamodb.Table('art')
    artTable.update_item(
        Key={'id': row_id},
        UpdateExpression="ADD #counter :increment",
        ExpressionAttributeNames={'#counter': 'counter'},
        ExpressionAttributeValues={':increment': {'N': '1'}}
    )

    artConfig = getConfig()

    redirect_url = artConfig['id']['redirect']

    response = {
        "statusCode": 302,
        "headers": {'Location': redirect_url},
        "body": json.dumps({})
    }

    return response



def getConfig():
    configTable = dynamodb.Table('Config')
    configKey = "HomePage"

    response = configTable.get_item(Key={'configKey': configKey})
    config = response['Item']['art']

    return config