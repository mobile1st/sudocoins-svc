import boto3
from art.art import Art
import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    # returns the art shared by the user
    user_id = event['rawPath'].replace('/arts/user/', '')
    my_art = art.get_uploads(user_id)

    return {
        'art': my_art
    }
