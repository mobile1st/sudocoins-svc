import boto3

from art.artprocessor.stream_to_s3 import safe_stream_to_s3


dynamodb = boto3.resource('dynamodb')
art_table = dynamodb.Table('art')
item_count = 0


def process_art(item):
    art_id = item.get("art_id")
    art_url = item.get("art_url")
    cdn_url = item.get("cdn_url")
    mime_type = item.get("mime_type")
    process_status = item.get("process_status")
    if not art_url:
        print(f'NO_ART_URL {art_id} => DELETE')
        art_table.delete_item(Key={'art_id': art_id})
    elif process_status == 'STREAM_TO_S3' or not cdn_url or mime_type == 'application/octet-stream':
        safe_stream_to_s3(art_id, art_url)


repeat = True
scan_res = art_table.scan()
while repeat:
    for art in scan_res['Items']:
        item_count += 1
        process_art(art)

    if 'LastEvaluatedKey' in scan_res:
        scan_res = art_table.scan(ExclusiveStartKey=scan_res['LastEvaluatedKey'])
    else:
        repeat = False

print(f'Processed {item_count} rows')
