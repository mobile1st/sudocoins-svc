import boto3
import json
import html
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    log.info(f'get_preview {event}')

    art_id = event['rawPath'].replace('/art/social/', '')
    user_agent = event['headers']['user-agent']
    art_object = get_by_share_id(art_id)

    log.info(f'user_agent {user_agent} art_id: {art_id} -> art: {art_object}')
    # if user_agent.find('facebookexternalhit') > -1:
    #     if not art_object:
    #         return {'statusCode': 404}
    #
    #     tags = get_twitter_html(art_object)
    #     return tags

    if user_agent.find('Twitterbot') > -1:
        score = user_agent.find('Twitterbot')
        log.info(f'score {score}')
        if not art_object:
            return {'statusCode': 404}

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'text/html'},
            'body': get_twitter_html(art_object)
        }

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
        return {
            'name': art_uploads_record['Item']['name'],
            'url': art_uploads_record['Item']['preview_url']
        }

    art_record = dynamodb.Table('art').get_item(
        Key={'art_id': share_id},
        ProjectionExpression="art_url, #n",
        ExpressionAttributeNames={'#n': 'name'})
    if 'Item' in art_record:
        return {
            'name': art_record['Item']['name'],
            'url': art_record['Item']['preview_url']
        }

    return None


def get_twitter_html(art):
    title = html.escape(art.get('name', ''))
    url = art.get('url', '')
    return f'<!DOCTYPE html>\
    <html lang="en">\
        <head>\
            <meta charset="utf-8" />\
            <title>Sudocoins</title>\
            <link rel="icon" href="/favicon.ico" />\
            <meta name="twitter:card" content="summary_large_image" />\
            <meta name="twitter:site" content="@sudocoins" />\
            <meta name="twitter:title" content="{title}" />\
            <meta name="twitter:description" content="Discover new Art and help creators grow"/>\
            <meta name="twitter:image" content="{url}" />\
        </head>\
        <body></body>\
    </html>'
