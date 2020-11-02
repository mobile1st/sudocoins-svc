import boto3
from botocore.exceptions import ClientError
import base64
import os
import json
from datetime import datetime


def lambda_handler(event, context):
    for record in event['Records']:
        payload = record['body']
        update(payload)

    return {
        "status": 200,
        "body": "success"
    }


def update(payload):
    # . data = payload
    data = json.loads(payload)
    transactionId = data["queryStringParameters"]['t']
    updated = str(datetime.utcnow().isoformat())

    try:
        payment, userId, revenue = getPayout(transactionId)

    except ClientError as e:
        print(e.response['Error']['Message'])
        payment = ""

    try:
        updateTransaction(transactionId, payment, data, updated, revenue)

        updateLedger(transactionId, payment, data, userId, updated)

    except ClientError as e:
        print(e)
        return None


def updateLedger(transactionId, payment, data, userId, updated):
    dynamodb = boto3.resource('dynamodb')
    ledgerTable = dynamodb.Table(os.environ["LEDGER_TABLE"])
    configTable = dynamodb.Table(os.environ["CONFIG_TABLE"])

    if not data["hashState"]:
        surveyCode = "F"
        payment = 0
    else:
        surveyCode = data["queryStringParameters"]["c"]

    surveyStatus = configTable.get_item(Key={'configKey': 'surveyStatus'})
    surveyStatus = surveyStatus['Item']["configValue"]["status"]

    if data["queryStringParameters"]["c"] in surveyStatus:
        ledgerStatus = surveyStatus[surveyCode]
    else:
        ledgerStatus = data["queryStringParameters"]["c"]

    data = ledgerTable.update_item(
        Key={
            'userId': userId,
            'transactionId': transactionId
        },
        UpdateExpression="set amount=:pay, #status1=:s, lastUpdate=:c",
        ExpressionAttributeValues={
            ":pay": payment,
            ":s": ledgerStatus,
            ":c": updated
        },
        ExpressionAttributeNames={
            "#status1": "status"
        },
        ReturnValues="UPDATED_NEW"
    )

    return data


def updateTransaction(transactionId, payment, data, updated, revenue):
    dynamodb = boto3.resource('dynamodb')
    transactionTable = dynamodb.Table(os.environ["TRANSACTION_TABLE"])

    if not data["hashState"]:
        surveyStatus = "F"
        revenue = 0
        payment = 0
    else:
        surveyStatus = data["queryStringParameters"]["c"]

    data = transactionTable.update_item(
        Key={
            'transactionId': transactionId
        },
        UpdateExpression="set payout=:pay, #status1=:s, completed=:c, redirect=:r, revenue=:rev",
        ExpressionAttributeNames={
            "#status1": "status"
        },
        ExpressionAttributeValues={
            ":pay": payment,
            ":s": surveyStatus,
            ":c": updated,
            ":r": data,
            ":rev": revenue
        },
        ReturnValues="UPDATED_NEW"
    )
    return data


def getPayout(transactionId):
    dynamodb = boto3.resource('dynamodb')
    transactionTable = dynamodb.Table(os.environ["TRANSACTION_TABLE"])
    configTable = dynamodb.Table(os.environ["CONFIG_TABLE"])

    transaction = transactionTable.get_item(Key={'transactionId': transactionId})
    buyerName = transaction['Item']['buyer']
    userId = transaction['Item']['userId']

    try:
        revenue = getSurveyRevenue(buyerName)
        revShare = configTable.get_item(Key={'configKey': 'revShare'})
        revShare = float(revShare['Item']["configValue"]["type"]["survey"])
        payment = str(float(revenue) * revShare)

    except Exception as e:
        print(e)
        payment = ""

    return payment, userId, revenue


def getSurveyRevenue(buyerName):
    dynamodb = boto3.resource('dynamodb')
    configTableName = os.environ["CONFIG_TABLE"]
    configTable = dynamodb.Table(configTableName)
    configKey = "TakeSurveyPage"

    try:
        response = configTable.get_item(Key={'configKey': configKey})
        configData = response['Item']["configValue"]

        if buyerName in configData["buyer"].keys():
            revenue = configData["buyer"][buyerName]["defaultCpi"]

            return revenue

        else:
            return ""

    except ClientError as e:
        print(e.response['Error']['Message'])

        return ""






