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
    if user_agent.find('Twitterbot') > -1 or user_agent.find('facebookexternalhit') > -1:
        if not art_object:
            return {'statusCode': 404}

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'text/html'},
            'body': get_preview_html(art_object)
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
        return art_uploads_record['Item']  # {name, art_url}

    art_record = dynamodb.Table('art').get_item(
        Key={'art_id': share_id},
        ProjectionExpression="art_url, #n",
        ExpressionAttributeNames={'#n': 'name'})
    if 'Item' in art_record:
        return art_record['Item']  # {name, art_url}

    return None


def get_preview_html(art):
    title = html.escape(art.get('name', ''))
    url = art.get('art_url', '')
    return f'<!DOCTYPE html>\
    <html lang="en" prefix="og: https://ogp.me/ns#">\
        <head>\
            <meta charset="utf-8" />\
            <title>Sudocoins</title>\
            <link rel="icon" href="/favicon.ico" />\
            <meta name="twitter:card" content="summary_large_image" />\
            <meta name="twitter:site" content="@sudocoins" />\
            <meta property="og:title" content="{title}" />\
            <meta property="og:description" content="Discover new Art and help creators grow" />\
            <meta property="og:image" content="{url}" />\
        </head>\
        <body></body>\
    </html>'
