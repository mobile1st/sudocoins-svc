import boto3
import json
from util import sudocoins_logger
from art.ledger import Ledger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
ledger = Ledger(dynamodb)


def lambda_handler(event, context):
    data = json.loads(event['Records'][0]['Sns']['Message'])
    log.info(f'payload: {data}')

    shareId = data['shareId']

    user_id = dynamodb.Table('art_uploads').get_item(
        Key={'shareId': shareId},
        ProjectionExpression="user_id")['Item']['user_id']

    dynamodb.Table('Profile').update_item(
        Key={'userId': user_id},
        UpdateExpression="SET sudocoins = if_not_exists(sudocoins, :start) + :inc",
        ExpressionAttributeValues={
            ':inc': 100,
            ':start': 0
        },
        ReturnValues="UPDATED_NEW"
    )

    ledger.add(100, user_id, 'Affiliate Link Signup')

    return shareId


