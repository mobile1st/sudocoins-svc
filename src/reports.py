import boto3


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    reportsTable = dynamodb.Table('Reports')

    reports = reportsTable.scan()

    return {
        "body": reports
    }
