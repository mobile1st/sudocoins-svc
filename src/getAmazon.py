import requests
from bs4 import BeautifulSoup
import random
from scraper_api import ScraperAPIClient
import lxml


def lambda_handler(event, context):
    url = event['url']
    cleanUrl = parseUrl(url)
    '''
    client = ScraperAPIClient('23697b4011e85f37992063c81f9e03ff')
    result = client.get(url = 'https://www.amazon.com/dp/B087CSW37P', country_code = "US").text
    soup = BeautifulSoup(result, "html.parser")
    price = soup.find("div", class_="a-section").find("span", class_="a-size-medium a-color-price")
    '''
    price, title, rating, imgUrl = loadAmazon(cleanUrl)

    return {
        'statusCode': 200,
        'body': {
            # "price": price,
            # "title": title,
            # "rating": rating,
            # "imgUrl": imgUrl
            "price": price.text
        }
    }


def loadAmazon(url):
    #  need process to change headers dynamically to avoid bot detection
    headers = ({'User-Agent':
                    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                    '(KHTML, like Gecko) Chrome/44.0.2403.157 '
                    'Safari/537.36',
                'Accept-Language': 'en-US, en;q=0.5'})
    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.text, "lxml")
    price = soup.find("span", attrs={'id': 'price_inside_buybox'}).string.strip()
    title = soup.find("span", attrs={'id': 'productTitle'}).string.strip()
    rating = soup.find("span", attrs={'class': 'a-icon-alt'}).string.strip()
    img = soup.find(id="imgTagWrapperId")
    imgUrl = img.findChild("img")['data-a-dynamic-image'].split(',')[0].split('"')[1]

    return price, title, rating, imgUrl


def parseUrl(url):
    productPrefix = url.find('/dp/')
    productStart = productPrefix + 4
    productEnd = url.find('/', productStart)
    productId = url[productStart:productEnd]
    finalUrl = 'https://www.amazon.com/dp/' + productId

    return finalUrl

