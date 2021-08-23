import boto3
import json
from sitemap import Sitemap
from boto3.dynamodb.conditions import Key
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art_table = dynamodb.Table('art')
s3 = boto3.resource('s3')
sitemap_bucket_name = 'sudocoins-sitemap-bucket'
sitemap_bucket = s3.Bucket(sitemap_bucket_name)


def lambda_handler(event, context):
    arts = event['arts'] if 'arts' in event else query_arts_for_indexing()

    log.info(f'upload to {sitemap_bucket_name} finished')


def get_sitemap_name():
    sitemaps = sitemap_bucket.objects.all()
    if not list(sitemaps):
        return 'sitemap-0.xml', 1

    for sitemap in sitemaps:
        print(sitemap.key, sitemap.last_modified)
    return 'sitemap-0.xml'


def query_arts_for_indexing():
    return art_table.query(
        KeyConditionExpression=Key('google_search_idx').eq('true'),
        Limit=10000,
        IndexName='google_search_index',
        ProjectionExpression='art_id, cdn_url'
    )['Items']


def upload_to_s3(sitemap: Sitemap):
    log.debug('uploading sitemaps to s3')
    response = s3.meta.client.put_object(
        Bucket=sitemap_bucket_name,
        Body=str(sitemap),
        Key=sitemap.get_name(),
        ContentType='application/xml'
    )
    log.debug(f'put_object response: {response}')


# lambda_handler(
#     {'arts': [
#         '03ab027e-f9ad-11eb-a118-3db60b974a7b',
#         '10c06d26-fb41-11eb-a956-772a464c46f1',
#         'f1bf6ee9-038f-11ec-b601-7974dad1a39c'
#     ]}, None
# )

def read_as_string(sitemap_obj_key):
    obj = sitemap_bucket.Object(sitemap_obj_key)
    return obj.get()['Body'].read().decode('utf-8')


def cucc():
    # upload_to_s3(Sitemap.from_art_ids('sitemap-0.xml', [
    #     '03ab027e-f9ad-11eb-a118-3db60b974a7b',
    #     '10c06d26-fb41-11eb-a956-772a464c46f1',
    #     'f1bf6ee9-038f-11ec-b601-7974dad1a39c'
    # ]))
    # get_sitemap_name()
    # read_as_string('sitemap-0.xml')
    sitemaps = sitemap_bucket.objects.filter(Prefix='sitemap')
    sorted_sitemaps = [obj.key for obj in sorted(sitemaps, key=lambda x: x.key)]
    print(sorted_sitemaps)
    # for sitemap in sorted_sitemaps:
    #     print(sitemap.key, sitemap.last_modified)

cucc()
