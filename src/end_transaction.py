import boto3
from botocore.exceptions import ClientError
import base64
import os
import json


def update_transaction(payload):
    dynamodb = boto3.resource('dynamodb')
    transaction_table = dynamodb.Table(os.environ["TRANSACTION_TABLE"])
    data = json.loads(payload)
    transactionId = "04ab3d2b-1188-11eb-8831-7f8847dd45ce"  # data['transaction_id']
    # hard coded for testing;

    try:
        response = transaction_table.get_item(Key={'TransactionId': transactionId})

    except ClientError as e:
        print(e.response['Error']['Message'])

    else:
        transaction = response['Item']
        defaultCPI = transaction["CPI"]
        transaction_table.update_item(
            Key={
                'TransactionId': transactionId
            },
            UpdateExpression="set Payout=:pay, #status1=:s, Completed=:c, Redirected=:r",
            ExpressionAttributeValues={
                ":pay": str(float(defaultCPI) * 0.8),
                ":s": data["status"],
                ":c": data["transaction_timestamp"],
                ":r": "Redirected"
            },
            ExpressionAttributeNames={
                "#status1": "status"
            },
            ReturnValues="UPDATED_NEW"
        )


def lambda_handler(event, context):
    for record in event['Records']:
        payload = record["body"]
        update_transaction(payload)
    return {
        "status": 200,
        "body": "success"
    }