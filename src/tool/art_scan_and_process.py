import boto3

from art.artprocessor.stream_to_s3 import stream_to_s3


dynamodb = boto3.resource('dynamodb')
art_table = dynamodb.Table('art')
item_count = 0


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
    art_url = item.get("art_url")
    cdn_url = item.get("cdn_url")
    process_status = item.get("process_status")
    if not art_url:
        print(f'NO_ART_URL {art_id} => DELETE')
        art_table.delete_item(Key={'art_id': art_id})
    elif process_status == 'STREAM_TO_S3' or not cdn_url:
        safe_stream_to_s3(art_id, item)


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
