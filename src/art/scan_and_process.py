import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import botocore
import requests


def stream_to_s3(data):
    dynamodb = boto3.resource('dynamodb')
    art_url = data['art_url']['S']
    response = requests.get(art_url, stream=True)

    if 'content-type' in response.headers:
        file_type = response.headers['content-type']
    else:
        file_type = None

    if 'content-length' in response.headers:
        file_size = response.headers['content-length']
    else:
        file_type = None

    type_index = file_type.find('/')
    file_ending = file_type[type_index + 1:]
    # print(response.headers)

    s3 = boto3.client('s3')
    s3_bucket = "art-processor-bucket"
    s3_file_path = data['art_id']['S'] + '.' + file_ending
    response.raw.decode_content = True
    conf = boto3.s3.transfer.TransferConfig(multipart_threshold=10000, max_concurrency=4)
    s3.upload_fileobj(response.raw, s3_bucket, s3_file_path, Config=conf)
    print('upload to s3 finished')

    art_table = dynamodb.Table('art')
    art_table.update_item(
        Key={'art_id': data['art_id']['S']},
        UpdateExpression="SET file_type=:ft, size=:size",
        ExpressionAttributeValues={
            ':ft': file_type,
            ':size': file_size
        },
        ReturnValues="UPDATED_NEW"
    )
    print("art file type and size added to art table")

    return


client2 = boto3.client('dynamodb')
response2 = client2.scan(
    TableName='art')
data = response2['Items']
while 'LastEvaluatedKey' in response2:
    response2 = client2.scan(TableName='art', ExclusiveStartKey=response2['LastEvaluatedKey'])
    data.extend(response2['Items'])

client = boto3.client('s3')
response = client.list_objects(
    Bucket='art-processor-bucket'
)
keys = []
for i in response['Contents']:
    index = i['Key'].find('.')
    keys.append(i['Key'][:index])
# print(keys)

bad_ids = []
count = 0
for i in data:
    try:
        if i['art_id']['S'] in keys:
            print("already added")
        else:
            print(i)
            stream_to_s3(i)

            count += 1
            print(count)
            print(i['art_id']['S'])
    except:
        bad_ids.append(i['art_id']['S'])
        continue

print(bad_ids)