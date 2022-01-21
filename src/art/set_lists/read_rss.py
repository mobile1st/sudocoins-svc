import boto3
from util import sudocoins_logger
import http.client
import hashlib
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")


def lambda_handler(event, context):
    cointelegraph = get_cointelegraph()
    decrypt = get_decrypt()
    blockchainnews = get_blockchainnews()

    news = cointelegraph + decrypt + blockchainnews
    add_news(news)

    return


def add_news(news_results):
    for i in news_results:
        dynamodb.Table('news').put_item(Item=i)

    log.info("news added to db")

    return


def get_cointelegraph():
    path = "/rss"
    conn = http.client.HTTPSConnection("cointelegraph.com")
    conn.request("GET", path)
    response = conn.getresponse()
    # log.info(f'response: {response}')
    decoded_response = response.read().decode('utf-8')
    # log.info(f'decoded_response: {decoded_response}')

    news_list = process_cointelegraph(decoded_response)

    return news_list


def get_decrypt():
    path = "/feed"
    conn = http.client.HTTPSConnection("decrypt.co")
    conn.request("GET", path)
    response = conn.getresponse()
    # log.info(f'response: {response}')
    decoded_response = response.read().decode('utf-8')
    # log.info(f'decoded_response: {decoded_response}')

    news_list = process_decrypt(decoded_response)

    return news_list


def get_blockchainnews():
    path = "/RSS?key=0HM9E1QNN797D"
    conn = http.client.HTTPSConnection("blockchain.news")
    conn.request("GET", path)
    response = conn.getresponse()
    # log.info(f'response: {response}')
    decoded_response = response.read().decode('utf-8')
    # log.info(f'decoded_response: {decoded_response}')

    news_list = process_blockchainnews(decoded_response)

    return news_list


def process_cointelegraph(rss_data):
    soup = BeautifulSoup(rss_data, "xml")
    news_list = soup.findAll("item")
    # log.info(news_list)

    news_objects = []
    for i in news_list:
        datetime_object = datetime.strptime(i.pubDate.text, '%a, %d %b %Y %H:%M:%S +0000')
        description = i.description.text
        index = description.find('</p><p>')
        description = description[index + 7:-17]

        msg = {
            "title": i.title.text,
            "pubDate": datetime_object,
            "description": description,
            "link": i.link.text,
            "media": i.find('content')['url']
        }

        category_list = i.findAll('category')
        categories = []
        for i in category_list:
            categories.append(str(i.contents[0]))
        msg['category'] = categories

        # log.info(msg)

        news_objects.append(msg)

    objects = []
    for i in news_objects:

        try:
            date = str(i.get('pubDate').isoformat())
            title = i.get('title')
            table_key = hashlib.md5((date + title).encode('utf-8'))
            msg = {}

            msg['id'] = str(table_key.hexdigest())
            msg['pubDate'] = date
            msg['link'] = i.get('link')
            msg['title'] = i.get('title')
            msg['approved'] = 'true'
            msg['source'] = 'cointelegraph'
            msg['media'] = i.get('media')
            msg['category'] = i.get('category')
            msg['description'] = i.get('description')

            objects.append(msg)


        except Exception as e:
            log.info(f'status - failure: {e}')

    return objects


def process_decrypt(rss_data):
    soup = BeautifulSoup(rss_data, "xml")
    news_list = soup.findAll("item")
    # log.info(news_list)

    news_objects = []
    for i in news_list:
        datetime_object = datetime.strptime(i.pubDate.text, '%a, %d %b %Y %H:%M:%S +0000')

        description = i.description.text
        # index = description.find('</p><p>')
        # description = description[index + 7:-17]

        msg = {
            "title": i.title.text,
            "pubDate": datetime_object,
            "description": description,
            "link": i.link.text,
            "media": i.find('thumbnail')['url']
        }

        category_list = i.findAll('category')
        categories = []
        for i in category_list:
            categories.append(str(i.contents[0]))
        msg['category'] = categories

        # log.info(msg)

        news_objects.append(msg)

    objects = []
    for i in news_objects:

        try:
            date = str(i.get('pubDate').isoformat())
            title = i.get('title')
            table_key = hashlib.md5((date + title).encode('utf-8'))
            msg = {}

            msg['id'] = str(table_key.hexdigest())
            msg['pubDate'] = date
            msg['link'] = i.get('link')
            msg['title'] = i.get('title')
            msg['approved'] = 'true'
            msg['source'] = 'decrypt'
            msg['media'] = i.get('media')
            msg['category'] = i.get('category')
            msg['description'] = i.get('description')

            objects.append(msg)


        except Exception as e:
            log.info(f'status - failure: {e}')

    return objects


def process_blockchainnews(rss_data):
    soup = BeautifulSoup(rss_data, "xml")
    news_list = soup.findAll("item")
    # log.info(news_list)

    news_objects = []
    for i in news_list:
        datetime_object = datetime.strptime(i.pubDate.text, '%a, %d %b %Y %H:%M:%S GMT')

        description = i.description.text
        index = description.find('<br />')
        index2 = description.find('<a ')
        description = description[index + 6:index2]

        msg = {
            "title": i.title.text,
            "pubDate": datetime_object,
            "description": description,
            "link": i.link.text,
            "media": i.find('thumbnail')['url']
        }

        category_list = i.findAll('category')
        categories = []
        for i in category_list:
            categories.append(str(i.contents[0]))
        msg['category'] = categories

        # log.info(msg)

        news_objects.append(msg)

    objects = []
    for i in news_objects:

        try:
            date = str(i.get('pubDate').isoformat())
            title = i.get('title')
            table_key = hashlib.md5((date + title).encode('utf-8'))
            msg = {}

            msg['id'] = str(table_key.hexdigest())
            msg['pubDate'] = date
            msg['link'] = i.get('link')
            msg['title'] = i.get('title')
            msg['approved'] = 'true'
            msg['source'] = 'blockchainnews'
            msg['media'] = i.get('media')
            msg['category'] = i.get('category')
            msg['description'] = i.get('description')

            objects.append(msg)


        except Exception as e:
            log.info(f'status - failure: {e}')

    return objects





