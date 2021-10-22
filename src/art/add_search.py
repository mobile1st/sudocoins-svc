import boto3
from util import sudocoins_logger
import string
import json

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")


def lambda_handler(event, context):
    art = json.loads(event['Records'][0]['Sns']['Message'])
    # art = event['Records'][0]['Sns']['Message']

    log.info(f'payload: {art}')

    collection_id = art.get('collection_id')
    if collection_id is None:
        return

    process_collection(collection_id)

    log.info(f'success')


def process_collection(collection_id):

    words = collection_id.split("-")

    for i in words:
        dynamodb.Table('search').update_item(
            Key={
                'search_key': i
            },
            UpdateExpression="SET collections = list_append(if_not_exists(collections, :empty_list), :i)",
            ExpressionAttributeValues={
                ':i': [collection_id],
                ':empty_list': []
            },
            ReturnValues="UPDATED_NEW"
        )

