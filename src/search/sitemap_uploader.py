import boto3
import http.client
from search.sitemap import Sitemaps
from boto3.dynamodb.conditions import Key
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art_table = dynamodb.Table('art')


def lambda_handler(event, context):
    manual_process = 'arts' in event
    arts = event['arts'] if manual_process else query_arts_for_indexing()
    if not arts:
        log.info('nothing to process')
        return

    log.info(f'processing {len(arts)} arts')
    sitemaps = Sitemaps()
    sitemaps.add(arts)
    sitemaps.write_sitemaps_to_s3()

    if not manual_process:
        remove_from_processing(arts)
        log.info('arts removed from google-search-index')

    notify_google(sitemaps)
    log.info(f'sitemap generation finished: {sitemaps}')


def query_arts_for_indexing():
    art_items = art_table.query(
        KeyConditionExpression=Key('process_to_google_search').eq('TO_BE_INDEXED'),
        IndexName='google-search-index',
        ProjectionExpression='art_id'
    )['Items']
    return [art['art_id'] for art in art_items]


def remove_from_processing(arts):
    for art_id in arts:
        art_table.update_item(
            Key={'art_id': art_id},
            UpdateExpression='REMOVE process_to_google_search',
        )


def notify_google(sitemaps):
    call_ping_endpoint(sitemaps.get_s3_url())
    for sitemap in sitemaps:
        if sitemap.is_new() or sitemap.is_modified():
            call_ping_endpoint(sitemap.get_s3_url())


def call_ping_endpoint(sitemap_url):
    log.info(f'pinging google for sitemap change: {sitemap_url}')
    conn = http.client.HTTPSConnection('www.google.com')
    conn.request('GET', f'/ping?sitemap={sitemap_url}')
    response = conn.getresponse().read()
    log.debug(f'google ping response: {response}')
