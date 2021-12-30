import boto3
from util import sudocoins_logger
import string
from boto3.dynamodb.conditions import Key

log = sudocoins_logger.get()
art_page_prefix = 'https://www.sudocoins.com/art/'
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    log.debug(f'event: {event}')
    query = extract_parameters(event['queryStringParameters'])
    log.info(query)

    key_list = []
    for i in query:
        key_list.append({"search_key": i.lower()})

    query = {
        'Keys': key_list,
        'ProjectionExpression': 'collections'
    }
    # log.info(query)

    response = dynamodb.batch_get_item(RequestItems={'search': query})
    # log.info(response)
    # log.info(response['Responses']['search'])
    if len(response['Responses']['search']) == 0:
        return {
            'arts': []
        }

    collections = set([])
    for k in response['Responses']['search']:
        # log.info(k)
        tmp_set = k['collections']
        collections.update(tmp_set)

    try:
        result = set(response['Responses']['search'][0]['collections'])
        log.info(result)
        check_list = list(set(l['collections']) for l in response['Responses']['search'][1:])
        for s in check_list:
            result = result.intersection(s)

    except e:
        print(e)

    # log.info(collections)

    return {
        'arts': get_collection_data(result)
    }


def extract_parameters(query_params):
    query = query_params['q']
    words = query.split(" ")

    return words


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def get_collection_data(collections):
    collection_objects = []

    key_list = []
    for i in collections:
        key_list.append({"collection_id": i})

    for k in [key_list[x:x + 100] for x in range(0, len(key_list), 100)]:
        query = {'Keys': k}
        log.info(query)

        response = dynamodb.batch_get_item(RequestItems={'collections': query})

        collection_objects = collection_objects + response['Responses']['collections']

    newlist = sorted(collection_objects, key=lambda d: d['sales_volume'], reverse=True)

    return newlist



