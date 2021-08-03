import boto3
from util import sudocoins_logger

log = sudocoins_logger.get()
kendra = boto3.client('kendra')
index_id = '8f96a3bb-3aae-476e-94ec-0d446877b42a'


def lambda_handler(event, context):
    log.debug(f'event: {event}')
    query_params = event['queryStringParameters']
    query = query_params['q']
    size = query_params.get('size')
    start = query_params.get('start')
    search_result = search(query, size, start)
    return {
        'total': search_result['TotalNumberOfResults'],
        'arts': [item['DocumentId'] for item in search_result['ResultItems']]
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def search(query, size=None, start=None):
    # TODO this hack is just for backward compatibility, should modify UI too
    page_size = int(size) if size else 10
    _start = (int(start) + 1) if start else 1
    page_number = int(_start / page_size) + 1 if _start >= page_size else 1
    response = kendra.query(
        IndexId=index_id,
        QueryText=query,
        QueryResultTypeFilter='DOCUMENT',
        PageNumber=page_number,
        PageSize=page_size
    )
    log.info(f'kendra response: {response}')
    return response
