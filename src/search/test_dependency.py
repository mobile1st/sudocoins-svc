import requests


def lambda_handler(event, context):
    print(requests.get('https://google.com'))
