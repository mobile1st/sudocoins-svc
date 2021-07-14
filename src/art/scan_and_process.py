import boto3

from art_processor import stream_to_s3


dynamodb_client = boto3.client('dynamodb')
item_count = 0

# TODO remove size attribute
# TODO review non image / video or zero byte downloads


def safe_stream_to_s3(art_id, item):
    art_url = item.get("art_url", {}).get('S')
    print(f'STREAM_TO_S3 {art_id} {art_url}')
    try:
        stream_to_s3(art_id, art_url)
    except Exception as e:
        print(f'FAILED to download {art_url} {e} Retry with alternative url')
        image_url = item.get("open_sea_data", {}).get('M', {}).get("image_url", {}).get('S', {})
        try:
            stream_to_s3(art_id, image_url)
        except Exception as e:
            print(f'FAILED to download fallback {image_url} {e}')


def process_art(item):
    art_id = item.get("art_id", {}).get('S')
    cdn_url = item.get("cdn_url", {}).get('S')
    file_type = item.get("file_type", {}).get('S')
    process_status = item.get("process_status", {}).get('S')
    if process_status:
        print(f'PRC_ST {art_id} -> {process_status} {cdn_url} {file_type}')
    if not cdn_url:
        print(f'NO_CDN {art_id} -> {process_status} {cdn_url} {file_type}')
    if 'html' in file_type or 'text' in file_type:
        print(f'NO_FT  {art_id} -> {process_status} {cdn_url} {file_type}')
    # if not cdn_url or 'html' in file_type or 'text' in file_type:
    #     return safe_stream_to_s3(art_id, item)


repeat = True
scan_res = dynamodb_client.scan(TableName='art')
while repeat:
    for art in scan_res['Items']:
        item_count += 1
        process_art(art)

    if 'LastEvaluatedKey' in scan_res:
        scan_res = dynamodb_client.scan(TableName='art', ExclusiveStartKey=scan_res['LastEvaluatedKey'])
    else:
        repeat = False

print(f'Processed {item_count} rows')
