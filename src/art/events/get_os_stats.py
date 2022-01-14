import boto3
from util import sudocoins_logger
import http.client
import json
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")


def lambda_handler(event, context):
    log.info("")

    data = dynamodb.Table('collections').query(
        KeyConditionExpression=Key('last_update').eq("false"),
        IndexName='last_update-index',
        ProjectionExpression='slug,collection_id'
    )

    collections = data['Items']

    while 'LastEvaluatedKey' in data:
        data = dynamodb.Table('collections').query(
            KeyConditionExpression=Key('last_update').eq("false"),
            IndexName='last_update-index',
            ProjectionExpression='slug,collection_id',
            ExclusiveStartKey=data['LastEvaluatedKey']
        )
        collections.extend(data['Items'])

    for i in collections:
        stats = call_open_sea(i['slug'])
        #update_expression1 = "SET floor = :fl, owners = :ow, last_update=:lu
        #update dynamodb
        log.info(stats)


    return


def call_open_sea(slug):
    path = "/api/v1/collection/"+slug+"/stats"
    log.info(f'path: {path}')
    conn = http.client.HTTPSConnection("api.opensea.io")
    api_key = {
        "X-API-KEY": "4714cd73a39041bf9cffda161163f8a5"
    }
    conn.request("GET", path, headers=api_key)
    response = conn.getresponse()
    decoded_response = response.read().decode('utf-8')
    open_sea_response = json.loads(decoded_response)

    return open_sea_response['asset_events']




