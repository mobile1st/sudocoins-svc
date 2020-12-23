import boto3


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    profile_table = dynamodb.Table("Profile")
    sub = event["sub"]
    if 'userId' in event and 'dynataConsent' in event:
        profile_table.update_item(
            Key={
                "userId": event['userId']
            },
            UpdateExpression="set dynataConsent=:dc",
            ExpressionAttributeValues={
                ":dc": True
            },
            ReturnValues="ALL_NEW"
        )

        return {
            'statusCode': 200,
            'body': "Consent Saved. Proceed to survey"
        }

    elif 'userId' in event and 'lucidConsent' in event:
        profile_table.update_item(
            Key={
                "userId": event['userId']
            },
            UpdateExpression="set lucidConsent=:lc, lucidLocale=:ll",
            ExpressionAttributeValues={
                ":lc": True,
                ":ll": event['locale']
            },
            ReturnValues="ALL_NEW"
        )

        return {
            'statusCode': 200,
            'body': "Consent Saved. Proceed to survey"
        }

    elif 'userId' in event and 'rfgConsent' in event:
        profile_table.update_item(
            Key={
                "userId": event['userId']
            },
            UpdateExpression="set lucidConsent=:lc, lucidProfile=:lp",
            ExpressionAttributeValues={
                ":lc": True,
                ":lp": event['profile']
            },
            ReturnValues="ALL_NEW"
        )


        return {
            'statusCode': 200,
            'body': "Consent Saved. Proceed to survey"
        }

    else:
        userId = loadProfile(sub)

        if userId is not None:
            data = profile_table.update_item(
                Key={
                    "userId": userId
                },
                UpdateExpression="set consent=:c",
                ExpressionAttributeValues={
                    ":c": event["consent"]

                },
                ReturnValues="ALL_NEW"
            )
            data['Attributes']['sub'] = sub

            return {
                'statusCode': 200,
                'body': data['Attributes']
            }

        else:

            return {
                'statusCode': 200,
                'body': "userId not found"
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
