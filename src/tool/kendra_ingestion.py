import boto3
import time
from art_document import ArtDocument

kendra = boto3.client('kendra')
dynamodb = boto3.resource('dynamodb')
rekognition = boto3.client('rekognition')

cdn_url_prefix = 'https://cdn.sudocoins.com/'

index_id = '8f96a3bb-3aae-476e-94ec-0d446877b42a'
data_source_id = '52596114-645e-40fa-b154-3ada7b3a7942'


def get_arts():
    art_table = dynamodb.Table('art')
    arts = []
    scan_kwargs = {}
    done = False
    start_key = None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = art_table.scan(**scan_kwargs)
        arts.extend(response.get('Items', []))
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None
    return arts


def get_docs(arts, job_execution_id):
    documents = []
    for item in arts:
        art_id = item['art_id']
        name = item.get('name')
        data = item.get('open_sea_data', {})
        desc = data.get('description')
        mime_type = item.get('mime_type')
        cdn_url = item.get('cdn_url')
        labels = None
        # if mime_type and cdn_url and (mime_type == 'image/jpeg' or mime_type == 'image/png'):
        #     labels = get_rekognition_labels(cdn_url.replace(cdn_url_prefix, ''))
        art_doc = ArtDocument() \
            .art_id(art_id) \
            .name(name) \
            .description(desc) \
            .rekognition_labels(labels)
        print(art_doc)
        documents.append(art_doc.to_kendra_doc(data_source_id, job_execution_id))
    return documents


def get_rekognition_labels(art_s3_name):
    try:
        response = rekognition.detect_labels(
            Image={
                'S3Object': {
                    'Bucket': 'sudocoins-art-bucket',
                    'Name': art_s3_name
                }
            },
            MinConfidence=70
        )
        # print(json.dumps(response, indent=4))
        return ','.join([label['Name'] for label in response['Labels']])
    except Exception as e:
        print(f'rekognition failed for {art_s3_name}, reason: {e}')
        return None


def ingest():
    # Start a data source sync job
    result = kendra.start_data_source_sync_job(
        Id=data_source_id,
        IndexId=index_id
    )
    print(f'Start data source sync operation: {result}')
    job_execution_id = result['ExecutionId']
    print(f'Job Execution ID: {job_execution_id}')

    # Start ingesting documents
    try:
        arts = get_arts()
        for batch in [arts[i:i + 10] for i in range(0, len(arts), 10)]:
            docs = get_docs(batch, job_execution_id)
            result = kendra.batch_put_document(
                IndexId=index_id,
                Documents=docs
            )
            print(f'Response from batch_put_document: {result}')
            time.sleep(1)

    # Stop data source sync job
    finally:
        result = kendra.stop_data_source_sync_job(
            Id=data_source_id,
            IndexId=index_id
        )
        print(f'Stop data source sync operation: {result}')


ingest()
