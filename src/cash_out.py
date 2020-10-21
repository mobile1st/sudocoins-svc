import json
from datetime import datetime
import boto3
from botocore.exceptions import ClientError


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    ledger_table = dynamodb.Table("Ledger")
    payout_table = dynamodb.Table("Payouts")
    # set time
    created_at = datetime.utcnow().isoformat()
    # payout object
    payout = {
        "PayoutID": "",  # auto-generated
        "UserId": "",  # from event
        "Amount": "",  # from event
        "CreatedAt": created_at,
        "Type": "",  # from event
        "Address": "",  # from event
        "Status": "pending"
    }
    # withdraw record added to ledger table
    withdraw = {
        "UserId": "",  # from event
        "Amount": "",  # from event
        "CreatedAt": created_at,
        "Type": "",  # from event
        "Status": "pending"
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
