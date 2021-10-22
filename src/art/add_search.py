import boto3
from util import sudocoins_logger
import json

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")


def lambda_handler(event, context):
    art = json.loads(event['Records'][0]['Sns']['Message'])
    # . art = event['Records'][0]['Sns']['Message']

    log.info(f'payload: {art}')

    collection_id = art.get('collection_id')
    if collection_id is None:
        return

    process_collection(collection_id)

    log.info(f'success')


def process_collection(collection_id):
    collection_name = collection_id.split(":")[1]
    log.info(f'collection_name: {collection_name}')
    words = collection_name.split("-")

    log.info(f'words: {words}')

    for i in words:
        dynamodb.Table('search').update_item(
            Key={
                'search_key': i
            },
            UpdateExpression="ADD collections :i",
            ExpressionAttributeValues={":i": set([collection_id])},
            ReturnValues="UPDATED_NEW"
        )


