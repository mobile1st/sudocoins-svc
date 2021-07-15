import boto3
from util import sudocoins_logger

log = sudocoins_logger.get()
client = boto3.client('cloudsearchdomain',
                      endpoint_url='https://search-art-domain-oemytuqtulkq5plos7ri5qhz7a.us-west-2.cloudsearch.amazonaws.com')


def lambda_handler(event, context):
    log.debug(f'event: {event}')
    query_params = event['queryStringParameters']
    query = query_params['q']
    size = int(query_params['size']) if query_params.get('size') else None
    start = int(query_params['start']) if query_params.get('start') else None
    search_result = search(query, size, start)
    return {
        'total': search_result['hits']['found'],
        'arts': [hit['id'] for hit in search_result['hits']['hit']]
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def search(query, size=None, start=None):
    response = client.search(
        query=query,
        returnFields='_no_fields',
        size=size if size else 10,
        start=start if start else 0
    )
    log.info(f'cloudsearch response: {response}')
    return response


int(None)
