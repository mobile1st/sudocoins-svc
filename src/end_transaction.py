import boto3
from botocore.exceptions import ClientError
import base64
import os


def update_transaction(payload):
    dynamodb = boto3.resource('dynamodb')
    transaction_table = dynamodb.Table(os.environ["TRANSACTION_TABLE"])
    try:
        response = transaction_table.get_item(Key={'TransactionId': payload["transaction_id"]})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        transaction = response['Item']
        defaultCPI = transaction["CPI"]
        transaction_table.update_item(
            Key={
                'TransactionId': payload["transaction_id"]
            },
            UpdateExpression="set Payout=:pay, status=:s, Completed=:c, Redirected=:r",
            ExpressionAttributeValues={
                ":pay": 100*defaultCPI*0.8,
                ":s": payload["status"],
                ":c": payload["transaction_timestamp"],
                ":r": "Redirected"
            },
            ReturnValues="UPDATED_NEW"
        )


# This lambda is called when a new record is in kinesis data stream.

def lambda_handler(event):
    # TODO implement
    print(event)
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record["kinesis"]["data"])
        print("Decoded payload: " + str(payload))
        update_transaction(payload)
    return {
        "status": 200,
        "body": "success"
    }