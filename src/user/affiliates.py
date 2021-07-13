import boto3
import json
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')


def lambda_handler(event, context):
    data = json.loads(event['Records'][0]['Sns']['Message'])
    log.info(f'payload: {data}')

    shareId = data['shareId']

    # look up shareId and see owner userId (art_uploads)
    # update affiliate partner's ledger using userId (ledger)
    # update affiliate partner's earned sudo in profile with userId (profile)

    return shareId


