import boto3
from util import sudocoins_logger
import http.client
import json
import hashlib
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")


def lambda_handler(event, context):
    news_results = call_rss_feeds()
    add_news()

    return


def call_rss_feeds():
    path = ""
    log.info(f'path: {path}')
    conn = http.client.HTTPSConnection("")
    conn.request("GET", path)
    response = conn.getresponse()
    # . log.info(f'response: {response}')
    decoded_response = response.read().decode('utf-8')
    # . log.info(f'response2: {response2}')
    rss_data = json.loads(decoded_response)

    process_rss(rss_data)

    return


def add_news(end_time):
    date = "this is an article about fish"  # insert internal user ID here
    title = "10-20-12"  # insert application key here

    table_key = hashlib.md5((date + title).encode('utf-8'))

    msg = {
        "id": table_key,
        "title": "",
        "pub_date": "",
        "media_content": "",
        "enclosure": "",
        "writer": "",
        "categories": [],
        "description": ""
    }

    dynamodb.Table('news').put_item()
    log.info("news table updated")


def process_rss(rss_data):
    page = rss_data
    soup = BeautifulSoup(page.content, "lxml")
    price = soup.find("span", attrs={'id': 'price_inside_buybox'}).string.strip()
    title = soup.find("span", attrs={'id': 'productTitle'}).string.strip()
    rating = soup.find("span", attrs={'class': 'a-icon-alt'}).string.strip()
    img = soup.find(id="imgTagWrapperId")
    imgUrl = img.findChild("img")['data-a-dynamic-image'].split(',')[0].split('"')[1]

    return price, title, rating, imgUrl


