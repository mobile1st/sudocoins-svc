from util import sudocoins_logger

log = sudocoins_logger.get()


def lambda_handler(event, context):
    log.debug(f'event: {event}')
    query_params = event['queryStringParameters']
    query = query_params['q']
    size = query_params.get('size')
    start = query_params.get('start')
    return {
        'total': 0,
        'arts': []
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))
