import boto3


def lambda_handler(event, context):
    jsonInput = event
    sub = jsonInput["sub"]

    nextStep, userId = loadProfile(sub)

    if nextStep is True:

        return {
            'statusCode': 200,
            'body': {
                "nextStep": "Continue to survey",
                "userId": userId
            }
        }

    elif nextStep is False:

        return {
            'statusCode': 200,
            'body': {
                "nextStep": "dynataConsent",
                "userId": userId
            }
        }

    else:

        return {
            'statusCode': 200,
            'body': {
                "nextStep": "Error, return to home"
            }
        }


def loadProfile(sub):
    dynamodb = boto3.resource('dynamodb')
    subTable = dynamodb.Table('sub')
    profileTable = dynamodb.Table('Profile')

    subResponse = subTable.get_item(Key={'sub': sub})

    if 'Item' in subResponse:
        userId = subResponse['Item']['userId']
        profileResponse = profileTable.get_item(Key={'userId': userId})
        if 'dynataConsent' in profileResponse['Item']:
            if profileResponse['Item']['dynataConsent']:
                return True, userId
            else:
                return False, userId
        else:
            return False, userId
    else:

        return None, None
