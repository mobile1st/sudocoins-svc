import json


def lambda_handler(event, context):
    print(event)

    clientId = event['callerContext']['clientId']
    userName = event['userName']
    code = event['request']['codeParameter']

    bodyUrl = 'https://api.sudocoins.com/account/verifyemail?' + "client_id=" + clientId + '&' + "user_name=" + userName + '&' + "confirmation_code=" + code

    event['response']["emailSubject"] = "Verify your email"
    event['response']["emailMessage"] = "Please click the link below to verify your email address:  " + bodyUrl

    print(event)

    return event