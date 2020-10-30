import boto3
from botocore.exceptions import ClientError
import base64
import os
import json
from datetime import datetime


def update(payload):
    dynamodb = boto3.resource('dynamodb')
    transactionTable = dynamodb.Table(os.environ["TRANSACTION_TABLE"])
    ledgerTable = dynamodb.Table(os.environ["LEDGER_TABLE"])
    configTable = dynamodb.Table(os.environ["CONFIG_TABLE"])
    # . data = payload
    data = json.loads(payload)
    transactionId = data["queryStringParameters"]['t']
    # updated = str(datetime.utcnow().isoformat())
    try:
        response = transactionTable.get_item(Key={'transactionId': transactionId})
        transaction = response['Item']
        buyerName = transaction['buyer']
        surveyObject = getSurveyObject(buyerName)
        revenue = surveyObject["defaultCpi"]

        revShare = configTable.get_item(Key={'configKey': 'revShare'})
        revShare = float(revShare['Item']["configValue"]["type"]["survey"])

    except ClientError as e:
        print(e.response['Error']['Message'])

    else:
        tdata = transactionTable.update_item(
            Key={
                'transactionId': transactionId
            },
            UpdateExpression="set Payout=:pay, #status1=:s, Completed=:c, Redirect=:r",
            ExpressionAttributeValues={
                ":pay": str(float(revenue) * revShare),
                ":s": data["queryStringParameters"]["c"],
                ":c": data["queryStringParameters"]["ts"],
                ":r": data
            },
            ExpressionAttributeNames={
                "#status1": "status"
            },
            ReturnValues="UPDATED_NEW"
        )

        surveyStatus = configTable.get_item(Key={'configKey': 'surveyStatus'})
        surveyStatus = surveyStatus['Item']["configValue"]["status"]
        if data["queryStringParameters"]["c"] in surveyStatus:
            userStatus = surveyStatus[data["queryStringParameters"]["c"]]
        else:
            userStatus = data["queryStringParameters"]["c"]

        ldata = ledgerTable.update_item(
            Key={
                'userId': transaction["userId"],
                'transactionId': transactionId
            },
            UpdateExpression="set amount=:pay, #status1=:s, lastUpdate=:c",
            ExpressionAttributeValues={
                ":pay": str(float(revenue) * revShare),
                ":s": userStatus,
                ":c": data["queryStringParameters"]["ts"]
            },
            ExpressionAttributeNames={
                "#status1": "status"
            },
            ReturnValues="UPDATED_NEW"
        )
        print(tdata, ldata)


def getSurveyObject(buyerName):
    configTableName = os.environ["CONFIG_TABLE"]
    configKey = "TakeSurveyPage"
    dynamodb = boto3.resource('dynamodb')
    configTable = dynamodb.Table(configTableName)
    try:
        response = configTable.get_item(Key={'configKey': configKey})
    except ClientError as e:
        print(e.response['Error']['Message'])
        return None
    else:
        try:
            configData = response['Item']["configValue"]
            if buyerName in configData["buyer"].keys():
                buyerObject = configData["buyer"][buyerName]
                return buyerObject
        except Exception as e:
            print(e)
            return None


def lambda_handler(event, context):
    for record in event['Records']:
        payload = record['body']
        update(payload)
    return {
        "status": 200,
        "body": "success"
    }
