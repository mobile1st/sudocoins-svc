import boto3


def lambda_handler(event, context):
    jsonInput = event
    sub = jsonInput["sub"]
    try:
        buyer = jsonInput['name']
    except Exception as e:
        buyer = 'dynata'

    userId, profile = loadProfile(sub)

    if buyer in ['dynata', 'peanutLabs']:
        if 'dynataConsent' in profile:
            if profile['dynataConsent']:
                nextStep = 'Continue to survey'
            else:
                nextStep = 'dynataConsent'
        else:
            nextStep = 'dynataConsent'
    elif buyer in ['lucid']:
        if 'lucidConsent' in profile:
            if profile['lucidConsent']:
                nextStep = 'Continue to survey'
            else:
                nextStep = "lucidConsent"
        else:
            nextStep = 'lucidConsent'
    elif buyer in ['rfg1', 'rfg2', 'rfg3']:
        if 'rfgConsent' in profile:
            if profile['rfgConsent']:
                nextStep = 'Continue to survey'
            else:
                nextStep = 'rfgConsent'
        else:
            nextStep = 'rfgConsent'
    else:
        nextStep = 'Continue to survey'

    if userId is None:
        nextStep = 'error'
        userId = 'error'

    return {
        'statusCode': 200,
        'body': {
            "nextStep": nextStep,
            "userId": userId
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
        profile = profileResponse['Item']
        return userId, profile
    else:
        return None, None
