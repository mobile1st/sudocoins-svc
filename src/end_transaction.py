import boto3
from botocore.exceptions import ClientError
import base64
import os
import json
from datetime import datetime
from rev_shares import Revshare
from decimal import Decimal


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
        payment, userId, revenue = getRevData(transactionId, data)

    except ClientError as e:
        print(e.response['Error']['Message'])
        payment = Decimal(0)
        revenue = Decimal(0)

    try:
        updateTransaction(transactionId, payment, data, updated, revenue)

        updateLedger(transactionId, payment, data, userId, updated)

    except ClientError as e:
        print(e)
        return None


def updateLedger(transactionId, payment, userId, updated, userStatus):
    dynamodb = boto3.resource('dynamodb')
    ledgerTable = dynamodb.Table(os.environ["LEDGER_TABLE"])

    updatedRecord = ledgerTable.update_item(
        Key={
            'userId': userId,
            'transactionId': transactionId
        },
        UpdateExpression="set amount=:pay, #status1=:s, lastUpdate=:c",
        ExpressionAttributeValues={
            ":pay": payment,
            ":s": userStatus,
            ":c": updated
        },
        ExpressionAttributeNames={
            "#status1": "status"
        },
        ReturnValues="UPDATED_NEW"
    )

    return updatedRecord


def updateTransaction(transactionId, payment, data, updated, revenue):
    dynamodb = boto3.resource('dynamodb')
    transactionTable = dynamodb.Table(os.environ["TRANSACTION_TABLE"])

    updatedRecord = transactionTable.update_item(
        Key={
            'transactionId': transactionId
        },
        UpdateExpression="set payout=:pay, #status1=:s, completed=:c, redirect=:r, revenue=:rev",
        ExpressionAttributeNames={
            "#status1": "status"
        },
        ExpressionAttributeValues={
            ":pay": payment,
            ":s": data["queryStringParameters"]["c"],
            ":c": updated,
            ":r": data,
            ":rev": revenue
        },
        ReturnValues="UPDATED_NEW"
    )
    return updatedRecord


def getRevData(transactionId, data):
    dynamodb = boto3.resource('dynamodb')
    transactionTable = dynamodb.Table(os.environ["TRANSACTION_TABLE"])


    transaction = transactionTable.get_item(Key={'transactionId': transactionId})
    buyerName = transaction['Item']['buyer']
    userId = transaction['Item']['userId']

    revData = Revshare(dynamodb)

    try:
        revenue, payment, userStatus = revData.get_revShare(data, buyerName)

    except Exception as e:
        print(e)
        payment = ""
        revenue = ""

    return payment, userId, revenue



