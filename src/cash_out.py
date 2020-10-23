import json
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import uuid


def lambda_handler(event, context):
    print(event)
    dynamodb = boto3.resource('dynamodb')
    ledger_table = dynamodb.Table("Ledger")
    payout_table = dynamodb.Table("Payouts")

    json_input = json.loads(event["body"])

    # set time
    created_at = datetime.utcnow().isoformat()
    # tid
    transactionId = str(uuid.uuid1())
    # payout object
    payout = {
        "paymentId": transactionId,
        "UserId": json_input["UserId"],
        "Amount": json_input["Amount"],
        "CreatedAt": created_at,
        "Type": json_input["Type"],
        "Address": json_input["Address"],
        "Status": "pending"
    }
    # withdraw record added to ledger table
    withdraw = {
        "UserId": json_input["UserId"],
        "Amount": json_input["Amount"],
        "CreatedAt": created_at,
        "Type": "Cash Out",
        "Status": "Pending",
        "TransactionId": transactionId
    }
    payout_response = payout_table.put_item(
        Item=payout
    )
    ledger_response = ledger_table.put_item(
        Item=withdraw
    )
    return {
        'statusCode': 200,
        'body': json.dumps('Cash out received')
    }
