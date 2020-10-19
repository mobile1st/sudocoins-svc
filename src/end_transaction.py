import boto3
from botocore.exceptions import ClientError
import base64
import os
import json


def update_transaction(payload):
    dynamodb = boto3.resource('dynamodb')
    transaction_table = dynamodb.Table(os.environ["TRANSACTION_TABLE"])
    data = base64.b64decode(payload["data"])
    data = str(data.decode("utf-8"))
    data = json.loads(data)
    transactionId = payload['partitionKey']
    try:
        response = transaction_table.get_item(Key={'TransactionId': "04ab3d2b-1188-11eb-8831-7f8847dd45ce"})
        print(response)
        #need a way to handle if transactionId isn't found with get_item
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        transaction = response['Item']
        defaultCPI = transaction["CPI"]
        transaction_table.update_item(
            Key={
                'TransactionId': transactionId
            },
            UpdateExpression="set Payout=:pay, status=:s, Completed=:c, Redirected=:r",
            ExpressionAttributeValues={
                ":pay": 100*float(defaultCPI)*0.8,
                ":s": data["status"],
                ":c": data["transaction_timestamp"],
                ":r": "Redirected"
            },
            ReturnValues="UPDATED_NEW"
        )


# This lambda is called when a new record is in kinesis data stream.

def lambda_handler(event, context):
    print(event)
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        #payload = base64.b64decode(record["kinesis"]["data"])
        payload = record["kinesis"]
        #print("Decoded payload: " + str(payload))
        update_transaction(payload)
    return {
        "status": 200,
        "body": "success"
    }