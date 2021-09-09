import boto3
from util import sudocoins_logger
import string

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
    # art_object = json.loads(event['Records'][0]['Sns']['Message'])
    art_object = event['Records'][0]['Sns']['Message']
    print(art_object)

    log.info(f'payload: {art_object}')
    art_id = art_object['art_id']

    text = art_object.get("name", None)
    a = text.translate(str.maketrans('', '', string.punctuation))
    b = a.split()
    c = [word.lower() for word in b]
    d = [w for w in c if not w in stop]

    for i in d:
        print(i)
        result = dynamodb.Table('search').update_item(
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
        print(result)

