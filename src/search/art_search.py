import json
import http.client
from util import sudocoins_logger

log = sudocoins_logger.get()
google_search_host = 'customsearch.googleapis.com'
search_engine_id = 'abe73c1ca8f9839de'
api_key = 'AIzaSyA4Be7eS9trAjcz5S4nkxPxKhhpC2IEP6E'
art_page_prefix = 'https://www.sudocoins.com/art/'


def lambda_handler(event, context):
    log.debug(f'event: {event}')
    query, size, start = extract_parameters(event['queryStringParameters'])
    # google search limitations
    if size > 10 or size + start > 100:
        return {
            'total': 0,
            'arts': []
        }

    search_response = call_google_search(query, size, start)
    log.info(f'search response: {json.dumps(search_response, indent=4)}')
    queries = search_response['queries']
    request = queries['request'][0]
    next_page_list = queries.get('nextPage')
    next_page = next_page_list[0] if next_page_list else {}
    arts = filter_results(search_response['items'])
    return {
        'total': request.get('totalResults', 0),
        'arts': arts,
        'filteredCount': len(arts),
        'unfilteredCount': request.get('count', 0),
        'nextPage': {
            'unfilteredCount': next_page.get('count', 0),
            'startIndex': next_page.get('startIndex', None)
        }
    }


def extract_parameters(query_params):
    query = query_params['q']
    size = int(query_params.get('size', 10))
    start = int(query_params.get('start', 1))
    return query, size, start


def call_google_search(query, size, start):
    conn = http.client.HTTPSConnection(google_search_host)
    conn.request('GET', f'/customsearch/v1?cx={search_engine_id}&key={api_key}&q={query}&num={size}&start={start}')
    response = conn.getresponse().read()
    log.debug(f'raw search response: {response}')
    return json.loads(response)


def filter_results(items):
    return [item['link'].replace(art_page_prefix, '') for item in items if item['link'].startswith(art_page_prefix)]


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))
