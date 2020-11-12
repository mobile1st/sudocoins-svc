import boto3
from botocore.exceptions import ClientError
import base64
import os
import json
from datetime import datetime
from rev_shares import RevenueData
from decimal import Decimal


def lambda_handler(event, context):
    for record in event['Records']:
        payload = record['body']
        update(payload)
        print("record updated")

    return {
        "status": 200,
        "body": "Success! Records pulled from queue and updated"
    }


def update(payload):
    data = json.loads(payload)
    transactionId = data["queryStringParameters"]['t']
    updated = str(datetime.utcnow().isoformat())

    try:
        payment, userId, revenue, userStatus = getRevData(transactionId, data)
        print("revData loaded")

    except ClientError as e:
        print(e.response['Error']['Message'])
        payment = Decimal(0)
        revenue = Decimal(0)

    try:
        updateTransaction(transactionId, payment, data, updated, revenue)
        print("Transaction updated")

    except ClientError as e:
        print(e)
        print("error updating Transaction table")

    try:
        updateLedger(transactionId, payment, userId, updated, userStatus)
        print("Ledger updated")

    except ClientError as e:
        print(e)
        print("error updating Ledger table")

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

    try:
        revData = RevenueData(dynamodb)
        revenue, payment, userStatus = revData.get_revShare(data, buyerName)
        print("revShare data from class loaded")

        return payment, userId, revenue, userStatus

    except Exception as e:
        print(e)
        payment = Decimal(0)
        revenue = Decimal(0)
        userStatus = ""
        print("revShare loaded from memory because of error")

        return payment, userId, revenue, userStatus





