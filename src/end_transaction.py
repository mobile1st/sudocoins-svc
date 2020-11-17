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

    #  cint or PL -- need to add more structure
    if 't' in data["queryStringParameters"]:
        transactionId = data["queryStringParameters"]['t']
    elif 'sub_id' in data["queryStringParameters"]:
        transactionId = data["queryStringParameters"]['sub_id']

    updated = str(datetime.utcnow().isoformat())

    try:
        payment, userId, revenue, userStatus, revShare = getRevData(transactionId, data)
        print("revData loaded")

    except ClientError as e:
        print(e.response['Error']['Message'])
        payment = Decimal(0)
        revenue = Decimal(0)

    try:
        updateTransaction(transactionId, payment, data, updated, revenue, revShare)
        print("Transaction updated")

    except ClientError as e:
        print(e)
        print("error updating Transaction table")

    try:
        if payment > 0:
            updateLedger(transactionId, payment, userId, updated, userStatus)
            print("Ledger updated")

    except ClientError as e:
        print(e)
        print("error updating Ledger table")

    return None


def updateLedger(transactionId, payment, userId, updated, userStatus):
    dynamodb = boto3.resource('dynamodb')
    ledgerTable = dynamodb.Table(os.environ["LEDGER_TABLE"])

    updatedRecord = ledgerTable.put_item(

        Item={
            'userId': userId,
            'transactionId': transactionId,
            'amount': payment,
            'status': userStatus,
            'lastUpdate': updated,
            'type': 'Survey'
        },
        ReturnValues="ALL_NEW"
    )
    print(updatedRecord)

    return updatedRecord


def updateTransaction(transactionId, payment, data, updated, revenue, revShare):
    dynamodb = boto3.resource('dynamodb')
    transactionTable = dynamodb.Table(os.environ["TRANSACTION_TABLE"])

    updatedRecord = transactionTable.update_item(
        Key={
            'transactionId': transactionId
        },
        UpdateExpression="set payout=:pay, #status1=:s, completed=:c, redirect=:r, revenue=:rev, revShare=:rs",
        ExpressionAttributeNames={
            "#status1": "status"
        },
        ExpressionAttributeValues={
            ":pay": payment,
            ":s": data["queryStringParameters"]["c"],
            ":c": updated,
            ":r": data,
            ":rev": revenue,
            ":rs": revShare
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
        revenue, payment, userStatus, revShare = revData.get_revShare(data, buyerName)
        print("revShare data from class loaded")

        return payment, userId, revenue, userStatus, revShare

    except Exception as e:
        print(e)
        payment = Decimal(0)
        revenue = Decimal(0)
        userStatus = ""
        revShare = Decimal(0)
        print("revShare loaded from memory because of error")

        return payment, userId, revenue, userStatus, revShare





