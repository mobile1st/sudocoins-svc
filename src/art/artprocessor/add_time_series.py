import boto3
from util import sudocoins_logger
import json


log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")


def lambda_handler(event, context):
    art = json.loads(event['Records'][0]['Sns']['Message'])
    log.info(f'art: {art}')

    timestamp = art['event_date'].split('T')[0]
    collection_id = art['collection_id']
    lsp = art['last_sale_price']
    art_id = art['art_id']
    if lsp == 0:
        log.info(f'sale price 0: {art_id}')
        return


    try:
        response = dynamodb.Table('time_series').update_item(
            Key={'date': timestamp, 'collection_id': collection_id},
            UpdateExpression="SET trades = list_append(if_not_exists(trades, :empty_list), :i),  "
                             "arts.#art = :k",
            ExpressionAttributeValues={
                ':i': [lsp],
                ':empty_list': [],
                ':k': lsp
            },
            ExpressionAttributeNames={
                '#art': art_id
            },
            ReturnValues="UPDATED_NEW"
        )

        log.info(response)

    except Exception as e:
        log.info(e)
        if e.response['Error']['Code'] == "ValidationException":
            dynamodb.Table('time_series').update_item(
                Key={'date': timestamp, 'collection_id': collection_id},
                UpdateExpression="SET trades = list_append(if_not_exists(trades, :empty_list), :i),  "
                                 "arts = if_not_exists(arts, :new_dict)",
                ExpressionAttributeValues={
                    ':i': [lsp],
                    ':empty_list': [],
                    ':new_dict': {}
                },
                ReturnValues="UPDATED_NEW"
            )
            dynamodb.Table('time_series').update_item(
                Key={'date': timestamp, 'collection_id': collection_id},
                UpdateExpression="SET arts.#art = :i",
                ExpressionAttributeValues={
                    ':i': lsp
                },
                ExpressionAttributeNames={
                    '#art': art_id
                },
                ReturnValues="UPDATED_NEW"
            )
        else:
            log.info("other error")

    return
