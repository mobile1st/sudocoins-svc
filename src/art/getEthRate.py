import boto3

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    rate = getConfig()

    return {
        'ethRate': rate
    }


def getConfig():
    configTable = dynamodb.Table('Config')
    configKey = "HomePage"

    response = configTable.get_item(Key={'configKey': configKey})
    config = response['Item']

    rate = str(config['ethRate'])

    return rate

