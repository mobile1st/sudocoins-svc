import boto3


def lambda_handler(event, context):
    """Updates the profile for a registered users.
    Arguments: user_name, twitter_handle, gravatarEmail
    Returns: fields updated
    """
    dynamodb = boto3.resource('dynamodb')
    profileTable = dynamodb.Table("Profile")

    if 'userId' in event:
        userId = event['userId']
    else:
        sub = event["sub"]
        userId = loadProfile(sub)

    profileQuery = profileTable.query(
        IndexName='user_name-index',
        KeyConditionExpression='user_name = :user',
        ExpressionAttributeValues={
            ':user': event['user_name']
        }
    )
    if profileQuery['Count'] > 0:
        return {
            "message": "User Name already exists. Please try something different."
        }

    data = profileTable.update_item(
        Key={
            "userId": userId
        },
        UpdateExpression="set currency=:c, gravatarEmail=:ge, user_name=:un, twitter_handle=:th",
        ExpressionAttributeValues={
            ":c": event["currency"],
            ":ge": event["gravatarEmail"],
            ":un": event['user_name'],
            ":th": event['twitter_handle'],

        },
        ReturnValues="ALL_NEW"
    )

    return {
        'body': data['Attributes']
    }


def loadProfile(sub):
    dynamodb = boto3.resource('dynamodb')
    subTable = dynamodb.Table('sub')

    subResponse = subTable.get_item(Key={'sub': sub})

    if 'Item' in subResponse:
        userId = subResponse['Item']['userId']

        return userId

    else:

        return None
