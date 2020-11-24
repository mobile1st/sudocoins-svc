import boto3
from history import History


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    subTable = dynamodb.Table('sub')
    subResponse = subTable.get_item(Key={'sub': event['sub']})

    if 'Item' in subResponse:
        print("founder userId matching sub")

        userId = subResponse['Item']['userId']

    loadHistory = History(dynamodb)
    moreHistory = loadHistory.getHistory(userId)


    return moreHistory[10:]
