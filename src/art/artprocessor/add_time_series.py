import boto3
from util import sudocoins_logger
import json
import statistics

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")


def lambda_handler(event, context):
    art = json.loads(event['Records'][0]['Sns']['Message'])
    # art = event['Records'][0]['Sns']['Message']
    log.info(f'art: {art}')

    timestamp = art['event_date'].split('T')[0]
    collection_id = art['collection_id']
    lsp = art['last_sale_price']

    dynamodb.Table('time_series').update_item(
        Key={'date': timestamp, 'collection_id': collection_id},
        UpdateExpression="SET trades = list_append(if_not_exists(trades, :empty_list), :i)",
        ExpressionAttributeValues={
            ':i': [lsp],
            ':empty_list': []
        },
        ReturnValues="UPDATED_NEW"
    )

    log.info(f'record updated')

    return
