import boto3
from util import sudocoins_logger
import string
import json

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")
stop = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself', 'yourselves',
        'he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their',
        'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are',
        'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an',
        'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about',
        'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up',
        'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when',
        'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no',
        'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don',
        'should', 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', 'couldn', 'didn', 'doesn', 'hadn', 'hasn',
        'haven', 'isn', 'ma', 'mightn', 'mustn', 'needn', 'shan', 'shouldn', 'wasn', 'weren', 'won', 'wouldn']


def lambda_handler(event, context):
    art = json.loads(event['Records'][0]['Sns']['Message'])
    # art = event['Records'][0]['Sns']['Message']

    log.info(f'payload: {art}')
    art_id = art.get('art_id')

    process_arts(art_id, art.get('name', ""), art.get('description', ""), art.get('collection_name', ""))
    # process_collections(art_id, art.get('name', ""), art.get('description', ""))

    log.info(f'success')


def process_arts(art_id, name, description, collection_name):
    if name is None:
        name = ""
    if description is None:
        description = ""
    if collection_name is None:
        collection_name = ""
    text = name + " " + description + " " + collection_name
    a = text.translate(str.maketrans('', '', string.punctuation))
    b = a.split()
    c = [word.lower() for word in b]
    d = [w for w in c if not w in stop]
    e = []
    for i in d:
        if i not in e:
            e.append(i)

    for i in e:
        dynamodb.Table('search').update_item(
            Key={
                'search_key': i
            },
            UpdateExpression="SET arts = list_append(if_not_exists(arts, :empty_list), :i)",
            ExpressionAttributeValues={
                ':i': [art_id],
                ':empty_list': []
            },
            ReturnValues="UPDATED_NEW"
        )


def process_collections(art_id, name, description):
    text = name + " " + description
    a = text.translate(str.maketrans('', '', string.punctuation))
    b = a.split()
    c = [word.lower() for word in b]
    d = [w for w in c if not w in stop]
    e = []
    for i in d:
        if i not in e:
            e.append(i)

    for i in e:
        dynamodb.Table('search').update_item(
            Key={
                'search_key': i
            },
            UpdateExpression="SET collections = list_append(if_not_exists(collections, :empty_list), :i)",
            ExpressionAttributeValues={
                ':i': [art_id],
                ':empty_list': []
            },
            ReturnValues="UPDATED_NEW"
        )

