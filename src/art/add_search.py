import boto3
from util import sudocoins_logger
import json
import re
from collections import Counter

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")


def words(text):
    return re.findall('[a-z]+', text.lower())


dictionary = Counter(words(open('big.txt').read()))
max_word_length = max(map(len, dictionary))


def lambda_handler(event, context):
    art = json.loads(event['Records'][0]['Sns']['Message'])
    # . art = event['Records'][0]['Sns']['Message']

    log.info(f'payload: {art}')

    collection_id = art.get('collection_id')
    if collection_id is None:
        return

    collection_name = collection_id.split(":")[1]
    log.info(f'collection_name: {collection_name}')
    words = collection_name.split("-")
    log.info(f'words before processing: {words}')

    for i in words:
        tmp_list = viterbi_segment(i)[0]
        words.append(tmp_list)

    tmp_set = set(words)
    words = list(tmp_set)

    process_collection(collection_id, words)

    log.info(f'success')


def process_collection(collection_id, words):


    for i in words:
        dynamodb.Table('search').update_item(
            Key={
                'search_key': i
            },
            UpdateExpression="ADD collections :i",
            ExpressionAttributeValues={":i": set([collection_id])},
            ReturnValues="UPDATED_NEW"
        )


def viterbi_segment(text):
    probs, lasts = [1.0], [0]
    print("1")
    for i in range(1, len(text) + 1):
        prob_k, k = max((probs[j] * word_prob(text[j:i]), j)
                        for j in range(max(0, i - max_word_length), i))
        probs.append(prob_k)
        lasts.append(k)
    words = []
    print(probs)
    print(lasts)
    i = len(text)
    while 0 < i:
        words.append(text[lasts[i]:i])
        i = lasts[i]
    words.reverse()
    return words, probs[-1]


def word_prob(word):
    return dictionary[word] / 466560










