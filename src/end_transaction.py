import boto3
from botocore.exceptions import ClientError
import base64
import os
import json
from datetime import datetime


def update(payload):
    dynamodb = boto3.resource('dynamodb')
    transaction_table = dynamodb.Table(os.environ["TRANSACTION_TABLE"])
    ledger_table = dynamodb.Table(os.environ["LEDGER_TABLE"])

    data = json.loads(payload)
    # print(data)
    transactionId = "255e183a-1494-11eb-ba43-6795a48f110e"  # data['transaction_id']
    # hard coded for testing;
    updated = str(datetime.utcnow().isoformat())

    try:
        response = transaction_table.get_item(Key={'TransactionId': transactionId})

    except ClientError as e:
        print(e.response['Error']['Message'])

    else:
        transaction = response['Item']
        revenue = transaction["Revenue"]
        tdata = transaction_table.update_item(
            Key={
                'TransactionId': transactionId
            },
            UpdateExpression="set Payout=:pay, #status1=:s, Completed=:c, Redirected=:r",
            ExpressionAttributeValues={
                ":pay": str(float(revenue) * 0.8),
                ":s": data["status"],
                ":c": updated,  # data["transaction_timestamp"],
                ":r": "Redirected"
            },
            ExpressionAttributeNames={
                "#status1": "status"
            },
            ReturnValues="UPDATED_NEW"
        )

        ldata = ledger_table.update_item(
            Key={
                'UserId': transaction["UserId"],
                'TransactionId': transactionId
            },
            UpdateExpression="set Amount=:pay, #status1=:s, Updated=:c",
            ExpressionAttributeValues={
                ":pay": str(float(revenue) * 0.8),
                ":s": data["status"],
                ":c": updated  # data["transaction_timestamp"]
            },
            ExpressionAttributeNames={
                "#status1": "status"
            },
            ReturnValues="UPDATED_NEW"
        )
        print(tdata, ldata)


def lambda_handler(event, context):
    for record in event['Records']:
        payload = record['body']  # json.dumps(record['body'])
        update(payload)
    return {
        "status": 200,
        "body": "success"
    }