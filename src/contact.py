import boto3
import json


def lambda_handler(event, context):
    """Send contact us message to DB
    Arguments: userId, message
    Returns: success message
    """
    dynamodb = boto3.resource('dynamodb')
    contactTable = dynamodb.Table("Contact")
    json_input = json.loads(event["body"])
    print(event)
    # . json_input = event["body"] #. uncomment for testing

    message = {
        json_input["userId"],
        json_input["message"]
    }
    contactResponse = contactTable.put_item(
        Item=message
    )

    return {
        'statusCode': 200,
        'body': json.dumps(message["Attributes"])
    }
