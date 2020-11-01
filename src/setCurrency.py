import boto3
import json


def lambda_handler(event, context):
    """Updates the currency for the user
    Arguments: userId, currency
    Returns: the new currency value
    """
    dynamodb = boto3.resource('dynamodb')
    profileTable = dynamodb.Table("Profile")
    json_input = json.loads(event["body"])
    print(event)
    # . json_input = event["body"] #. uncomment for testing

    data = profileTable.update_item(
        Key={
            "userId": json_input["userId"]
        },
        UpdateExpression="set currency=:c",
        ExpressionAttributeValues={
            ":c": json_input["currency"]
        },
        ReturnValues="ALL_NEW"
    )

    return {
        'statusCode': 200,
        'body': json.dumps(data["Attributes"])
    }
