import boto3
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    return
