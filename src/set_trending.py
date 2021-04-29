import boto3
from art import Art

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):

    arts = ["0x672b4d3e393e99661922ff0fb0d6b32be13faba3#1",
            "0x60f80121c31a0d46b5279700f9df786054aa5ee5#853300",
            "0x672b4d3e393e99661922ff0fb0d6b32be13faba3#2",
            "0x672b4d3e393e99661922ff0fb0d6b32be13faba3#1"]

    art_uploads_record = art.get_arts(arts)

    print(art_uploads_record)

    return {
        'statusCode': 200,
        'body': art_uploads_record
    }