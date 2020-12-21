import boto3
import json
from transaction import Transaction


def lambda_handler(event, context):
    try:
        for record in event['Records']:
            payload = record['body']
            print(payload)

            update(payload)
            print("record updated")

        return {
            "status": 200,
            "body": "Success! Records pulled from queue and updated"
        }

    except Exception as e:
        print(e)

        return {
            "status": 200,
            "body": "Failure! Something went wrong"
        }


def update(payload):
    data = json.loads(payload)
    dynamodb = boto3.resource('dynamodb')
    transaction = Transaction(dynamodb)

    if data["buyerName"] == 'cint' or data["buyerName"] == 'test':
        transaction.endTest(data)

    elif data["buyerName"] == 'lucid':
        transaction.endLucid(data)

    elif data["buyerName"] == 'peanutLabs':
        transaction.endPL(data)

    elif data["buyerName"] == 'dynata':
        transaction.endDynata(data)



'''
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


'''


