import boto3
from util import sudocoins_logger

log = sudocoins_logger.get()
google_search_host = 'customsearch.googleapis.com'
search_engine_id = 'abe73c1ca8f9839de'
api_key = 'AIzaSyA4Be7eS9trAjcz5S4nkxPxKhhpC2IEP6E'
art_page_prefix = 'https://www.sudocoins.com/art/'
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    log.debug(f'event: {event}')
    query = extract_parameters(event['queryStringParameters'])
    log.info(query)
    arts = []
    for i in query:
        log.info(i)
        tmp = dynamodb.Table('search').get_item(
            Key={'search_key': i},
            ProjectionExpression="arts")
        log.info(tmp)

        if 'Item' in tmp:
            tmp = tmp['Item']['arts']

            for k in tmp:
                if k not in arts:
                    arts.append(k)

    return {
        'arts': arts
    }


def extract_parameters(query_params):
    query = query_params['q']
    words = query.split(" ")

    return words


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))
