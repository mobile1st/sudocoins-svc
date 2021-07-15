import boto3

from art.art_processor import stream_to_s3


dynamodb = boto3.resource('dynamodb')
art_table = dynamodb.Table('art')
item_count = 0

# TODO review non-image / video or zero byte downloads


def safe_stream_to_s3(art_id, item):
    art_url = item.get("art_url")
    print(f'STREAM_TO_S3 {art_id} {art_url}')
    try:
        stream_to_s3(art_id, art_url)
    except Exception as e:
        print(f'FAILED to download {art_url} {e} Retry with alternative url')
        image_url = item.get("open_sea_data", {}).get("image_url")
        try:
            stream_to_s3(art_id, image_url)
        except Exception as e:
            print(f'FAILED to download fallback {image_url} {e}')


def process_art(item):
    art_id = item.get("art_id")
    cdn_url = item.get("cdn_url")
    process_status = item.get("process_status")
    if process_status == 'STREAM_TO_S3' or not cdn_url:
        safe_stream_to_s3(art_id, item)
    if 'image' not in item.get("mime_type"):
        print(f'{art_id} -> {item["mime_type"]}')


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
