import json
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import uuid


def lambda_handler(event, context):
    print(event)
    dynamodb = boto3.resource('dynamodb')
    ledgerTable = dynamodb.Table("Ledger")
    payoutTable = dynamodb.Table("Payouts")

    jsonInput = (event)

    lastUpdate = datetime.utcnow().isoformat()
    transactionId = str(uuid.uuid1())

    payout = {
        "paymentId": transactionId,
        "userId": jsonInput["userId"],
        "amount": jsonInput["amount"],
        "lastUpdate": lastUpdate,
        "type": jsonInput["type"],
        "address": jsonInput["address"],
        "Status": "Pending"
    }
    # withdraw record added to ledger table
    withdraw = {
        "userId": jsonInput["userId"],
        "amount": jsonInput["amount"],
        "lastUpdate": lastUpdate,
        "type": "Cash Out",
        "status": "Pending",
        "transactionId": transactionId
    }
    payout_response = payoutTable.put_item(
        Item=payout
    )
    ledger_response = ledgerTable.put_item(
        Item=withdraw
    )
    return {
        'statusCode': 200,
        'body': json.dumps('Cash out received')
    }
