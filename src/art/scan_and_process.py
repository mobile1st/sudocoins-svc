import boto3
import json
import mimetypes
import http.client
from urllib.parse import urlparse
import ssl


dynamodb = boto3.resource('dynamodb')
ssl._create_default_https_context = ssl._create_unverified_context


def stream_to_s3(data):
    art_table = dynamodb.Table('art')
    art_table.update_item(
        Key={'art_id': data['art_id']['S']},
        UpdateExpression="SET process_status=:ps",
        ExpressionAttributeValues={
            ':ps': "attempted"
        },
        ReturnValues="UPDATED_NEW"
    )
    file = download(data['art_url']['S'])

    s3_bucket = "art-processor-bucket"
    s3_file_path = data['art_id']['S'] + mimetypes.guess_extension(file['mimeType'])
    client = boto3.client('s3')
    client.put_object(Bucket=s3_bucket, Body=file['bytes'], Key=s3_file_path, ContentType=file['mimeType'])

    art_table.update_item(
        Key={'art_id': data['art_id']['S']},
        UpdateExpression="SET file_type=:ft, size=:size, process_status=:ps",
        ExpressionAttributeValues={
            ':ft': file['mimeType'],
            ':size': len(file['bytes']),
            ':ps': "processed"
        },
        ReturnValues="UPDATED_NEW"
    )

    print("file uploaded")

    return


def download(url: str):
    url = urlparse(url)
    conn = http.client.HTTPSConnection(url.hostname, timeout=15)
    conn.request("GET", url.path)
    response = conn.getresponse()
    length = response.getheader('Content-Length')
    content_type = response.getheader('Content-Type')
    content_bytes = response.read()
    return {'mimeType': content_type, 'bytes': content_bytes}



client2 = boto3.client('dynamodb')
response2 = client2.scan(
TableName='art')
data = response2['Items']
while 'LastEvaluatedKey' in response2:
    response2 = client2.scan(TableName='art',ExclusiveStartKey=response2['LastEvaluatedKey'])
    data.extend(response2['Items'])


client = boto3.client('s3')
response = client.list_objects(
    Bucket='art-processor-bucket'
)
keys = []
for i in response['Contents']:
    index = i['Key'].find('.')
    keys.append(i['Key'][:index])


bad_ids = []
count = 0
count2 = 0
for i in data:
    try:
        if i['art_id']['S'] in keys:
            count2 += 1
            print("added count: " + str(count2))
        else:
            stream_to_s3(i)
            count += 1
            print("uploaded count: " + str(count))
            #print(i['art_id']['S'])
    except Exception as e:
        print(e)
        bad_ids.append(i['art_id']['S'])
        continue

print(bad_ids)
