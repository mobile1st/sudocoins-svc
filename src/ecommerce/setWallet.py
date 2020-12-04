import boto3
from wyre import Wyre


def lambda_handler(event, context):
    print(event)

    for record in event['Records']:
        payload = record['body']
        print(payload)

        walletId, walletAdrress = callWyre(payload['currency'], payload['orderId'])

        updateOrder(payload['orderId'], walletId, walletAdrress)

    return {
        "status": 200,
        "body": "Success! Order pulled from Queue, Wyre wallet created, Order updated"
    }


def callWyre(currency, orderId):
    payload = {
        "name": orderId,
        "callbackURL": 'https://j6d32mcwfd.execute-api.us-west-2.amazonaws.com/default/wyrecallback'
    }

    wyre = Wyre()
    response = wyre.createWallet(payload)

    walletId = response['srn']

    if currency == 'BTC':
        walletAdrress = response['depositAddresses']['BTC']
    else:
        walletAdrress = response['depositAddresses']['ETH']

    return walletId, walletAdrress


def updateOrder(orderId, walletId, walletAddress):
    dynamodb = boto3.resource('dynamodb')
    ordersTable = dynamodb.Table('orders')

    ordersTable.update_item(
        Key={
            "orderId": orderId
        },
        UpdateExpression="set walletId=:wid, walletAdrress=:wad",
        ExpressionAttributeValues={
            ":wid": walletId,
            ":wad": walletAddress
        },
        ReturnValues="ALL_NEW"
    )
