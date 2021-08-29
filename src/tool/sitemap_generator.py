import boto3
from search import sitemap_uploader

dynamodb = boto3.resource('dynamodb')


def get_art_ids():
    art_table = dynamodb.Table('art')
    arts = []
    scan_kwargs = {'FilterExpression': 'attribute_not_exists(process_to_google_search)'}
    done = False
    start_key = None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = art_table.scan(**scan_kwargs)
        arts.extend([art['art_id'] for art in response.get('Items', [])])
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None
    return arts


def generate_sitemaps():
    art_ids = get_art_ids()
    print(art_ids)
    sitemap_uploader.lambda_handler({'arts': art_ids}, None)


generate_sitemaps()
