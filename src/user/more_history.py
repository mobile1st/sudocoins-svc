import boto3
from art.history import History
import json


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    request_data = json.loads(event['body'])
    subTable = dynamodb.Table('sub')
    sub = request_data['sub']
    subResponse = subTable.get_item(Key={'sub': sub})

    if 'Item' in subResponse:
        print("founder userId matching sub")

        userId = subResponse['Item']['userId']

    loadHistory = History(dynamodb)
    moreHistory = loadHistory.getHistory(userId)


    return moreHistory[10:]
