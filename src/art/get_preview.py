import boto3
import json
from util import sudocoins_logger
from art.art import Art

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    log.info(f'get_preview {event}')

    art_id = event['rawPath'].replace('/art/social/', '')
    user_agent = event['headers']['user-agent']
    log.info(f'user_agent {user_agent}')

    if user_agent.find('facebookexternalhit') > -1:
        art_object = get_by_share_id(art_id)
        tags = get_html(art_object['name'], art_object['art_url'])
        return tags

    elif user_agent.find('Twitterbot') > -1:
        score = user_agent.find('Twitterbot')
        log.info(f'score {score}')
        art_object = get_by_share_id(art_id)
        tags = get_html(art_object['name'], art_object['art_url'])
        log.info(f'html {tags}')
        return tags

    else:
        return {
            "statusCode": 302,
            "headers": {'Location': ('https://www.sudocoins.com/art/' + art_id)},
            "body": json.dumps({})
        }


def get_by_share_id(share_id):
    # returns the art_uploads record based on shareId
    art_uploads_record = dynamodb.Table('art_uploads').get_item(
        Key={'shareId': share_id},
        ProjectionExpression="art_url, #n",
        ExpressionAttributeNames={'#n': 'name'}
    )
    if 'Item' in art_uploads_record:
        return art_uploads_record['Item']

    art_record = dynamodb.Table('art').get_item(
        Key={'art_id': share_id},
        ProjectionExpression="art_url, #n",
        ExpressionAttributeNames={'#n': 'name'})
    if 'Item' in art_record:
        return art_record['Item']

    return {
        "message": "Art not found. Add generic preview data"
    }


def get_html(title, image):
    twitter_card = "<meta name = \"twitter:card\" content = \"summary_large_image\" >"
    site = "< meta name = \"twitter:site\" content = \"@sudocoins\" >"
    title = "< meta name = \"twitter:title\" content = " + title + " >"
    description = "< metabname = \"twitter:description\" content = \"Discover new Art and help creators grow\" >"
    image = "< meta name = \"twitter:image\" content = " + image + ">"

    return '<html><head>' + twitter_card + site + \
           title + description + image + '</head><body></body></html>'
