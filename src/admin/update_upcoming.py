import boto3
import json
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')



def lambda_handler(event, context):
    body = json.loads(event['body'])
    #. body = event
    log.debug(f'data: {body}')

    if body['status'] == "approve":
        status = "true"
    elif body['status'] == "disapprove":
        status = "disapproved"

    response = dynamodb.Table('upcoming').update_item(
        Key={
            "upcoming_id": body['upcoming_id']
        },
        UpdateExpression="set approved=:app",
        ExpressionAttributeValues={
            ":app": status
        },
        ReturnValues="UPDATED_NEW"
    )


    return {
        "response": response
    }




