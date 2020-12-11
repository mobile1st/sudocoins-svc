import json
import requests


def lambda_handler(event, context):
    print(event)

    params = event["queryStringParameters"]

    url = 'https://auth.app.sudocoins.com/confirmUser?'
    clientId = params['client_id']
    userName = params['user_name']
    code = params['confirmation_code']

    finalUrl = url + clientId + '&' + userName + '&' + code

    response = requests.get(finalUrl)
    (print(response.text.encode('utf8')))

    response = {
        "statusCode": 302,
        "headers": {'Location': 'https://www.sudocoins.com/?msg=welcome'},
        "body": json.dumps({})
    }
