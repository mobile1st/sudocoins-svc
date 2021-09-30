import boto3
from util import sudocoins_logger
import json

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")


def lambda_handler(event, context):
    art = json.loads(event['Records'][0]['Sns']['Message'])
    # art = event['Records'][0]['Sns']['Message']

    log.info(f'received message')

    return