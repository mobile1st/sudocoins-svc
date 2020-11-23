import boto3
from botocore.exceptions import ClientError
import base64
import os
import json
from datetime import datetime
from rev_shares import RevenueData
from decimal import Decimal
from history import History


def lambda_handler(event, context):
    for record in event['Records']:
        payload = record['body']
        print(payload)
        update(payload)
        print("record updated")

    return {
        "status": 200,
        "body": "Success! Records pulled from queue and updated"
    }


def update(payload):
    data = json.loads(payload)
    dynamodb = boto3.resource('dynamodb')
    history = History(dynamodb)

    if data["buyerName"] == 'cint':
        transactionId = data["queryStringParameters"]['t']
        surveyCode = data["queryStringParameters"]['c']

    elif data["buyerName"] == 'peanutLabs':
        transactionId = data["queryStringParameters"]['sub_id']
        surveyCode = data["queryStringParameters"]['status']

    updated = str(datetime.utcnow().isoformat())

    try:
        payment, userId, revenue, userStatus, revShare, cut = getRevData(transactionId, data)
        print("revData loaded")

    except ClientError as e:
        print(e.response['Error']['Message'])
        payment = Decimal(0)
        revenue = Decimal(0)

    try:
        history.updateTransaction(transactionId, payment, surveyCode, updated,
                                  revenue, revShare, userStatus, cut, data, userId)
        print("Transaction updated")

    except ClientError as e:
        print(e)
        print("error updating Transaction table")

    try:
        if payment > 0:
            history.createLedgerRecord(transactionId, payment, userId, updated, userStatus)
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
        }
    )
    print(updatedRecord)

    return updatedRecord


def updateTransaction(transactionId, payment, surveyCode, updated, revenue, revShare, userStatus, cut, data):
    dynamodb = boto3.resource('dynamodb')
    transactionTable = dynamodb.Table(os.environ["TRANSACTION_TABLE"])

    updatedRecord = transactionTable.update_item(
        Key={
            'transactionId': transactionId
        },
        UpdateExpression="set payout=:pay, #status1=:s, completed=:c, redirect=:r, revenue=:rev, revShare=:rs, "
                         "surveyCode=:sc",
        ExpressionAttributeNames={
            "#status1": "status"
        },
        ExpressionAttributeValues={
            ":pay": payment,
            ":s": userStatus,
            ":c": updated,
            ":r": data,
            ":rev": revenue*cut,
            ":rs": revShare,
            ":sc": surveyCode
        },
        ReturnValues="UPDATED_NEW"
    )
    return updatedRecord


def getRevData(transactionId, data):
    dynamodb = boto3.resource('dynamodb')
    transactionTable = dynamodb.Table(os.environ["TRANSACTION_TABLE"])

    transaction = transactionTable.get_item(Key={'transactionId': transactionId})

    buyerName = data['buyerName']
    userId = transaction['Item']['userId']

    try:
        revData = RevenueData(dynamodb)
        print("about to call get_Revshare")
        revenue, payment, userStatus, revShare, cut = revData.get_revShare(data, buyerName)
        print("revShare data from class loaded")

        return payment, userId, revenue, userStatus, revShare, cut

    except Exception as e:
        print(e)
        payment = Decimal(0)
        revenue = Decimal(0)
        userStatus = ""
        revShare = Decimal(0)
        cut = Decimal(0)
        print("revShare loaded from memory because of error")

        return payment, userId, revenue, userStatus, revShare, cut





