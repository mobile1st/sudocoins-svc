import boto3
import html
import http.client
from urllib.parse import urlparse
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    user_agent = event['headers']['user-agent']
    art_id = event['rawPath'].replace('/art/social/', '')
    log.info(f'user_agent {user_agent} art_id: {art_id}')

    if is_browser(user_agent):
        return {
            "statusCode": 302,
            "headers": {'Location': ('https://www.sudocoins.com/art/' + art_id)},
        }

    art = get_by_share_id(art_id)
    if not art:
        log.warn(f'Could not find art for art_id {art_id}')
        return {'statusCode': 404}

    url = get_preview_url(art)
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'text/html'},
        'body': get_preview_html(art['name'], url)
    }


def is_browser(user_agent):
    return 'Twitterbot' not in user_agent and 'facebookexternalhit' not in user_agent


def get_preview_url(art):
    preview = art['preview_url']
    if 'googleusercontent.com/' in preview:
        # google can do the resizing
        parts = preview.split('=')
        return f'{parts[0]}=w1200-h628-p'

    # orig and if > 5MB otherwise use preview
    url = urlparse(art['art_url'])
    conn = http.client.HTTPSConnection(url.hostname)
    conn.request("HEAD", url.path)
    resp = conn.getresponse()
    length = int(resp.getheader('Content-Length'))

    return art['art_url'] if length < 5000000 else art['preview_url']


def get_by_share_id(share_id):
    """ returns the art urls based on shareId: {name, art_url, preview_url} """
    art_uploads_record = dynamodb.Table('art_uploads').get_item(
        Key={'shareId': share_id},
        ProjectionExpression="art_url, preview_url, #n",
        ExpressionAttributeNames={'#n': 'name'}
    )
    if 'Item' in art_uploads_record:
        return art_uploads_record['Item']

    art_record = dynamodb.Table('art').get_item(
        Key={'art_id': share_id},
        ProjectionExpression="art_url, preview_url, #n",
        ExpressionAttributeNames={'#n': 'name'})
    if 'Item' in art_record:
        return art_record['Item']

    return None


def get_preview_html(title, url):
    title = html.escape(title)
    return f"""<!DOCTYPE html>
    <html lang="en" prefix="og: https://ogp.me/ns#">
        <head>
            <meta charset="utf-8" />
            <title>Sudocoins</title>
            <link rel="icon" href="/favicon.ico" />
            <meta name="twitter:card" content="summary_large_image" />
            <meta name="twitter:site" content="@sudocoins" />
            <meta property="og:title" content="{title}" />
            <meta property="og:description" content="Discover new Art and help creators grow" />
            <meta property="og:image" content="{url}" />
        </head>
        <body></body>
    </html>"""
