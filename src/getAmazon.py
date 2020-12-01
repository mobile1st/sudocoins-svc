from bs4 import BeautifulSoup
from scraper_api import ScraperAPIClient
import datetime


def lambda_handler(event, context):
    url = event['url']
    print("cleaning url")
    print(datetime.datetime.now())
    print("")

    cleanUrl = parseUrl(url)

    print('url cleaned')
    print(datetime.datetime.now())
    print("")

    print("calling loadAmazon")
    print(datetime.datetime.now())
    print("")

    price, title = loadAmazon(cleanUrl)  # , rating, imgUrl

    print("done parsing")
    print(datetime.datetime.now())
    print("")

    return {
        'statusCode': 200,
        'body': {
            "price": price,
            "title": title  # ,
            # "rating": rating,
            # "imgUrl": imgUrl

        }
    }


def loadAmazon(url):
    print("creating client")
    print(datetime.datetime.now())
    print("")

    client = ScraperAPIClient('23697b4011e85f37992063c81f9e03ff')

    print("client created")
    print(datetime.datetime.now())
    print("")

    print("calling scraper")
    print(datetime.datetime.now())
    print("")

    page = client.get(url=url, country_code="US")

    print("receive response")
    print(datetime.datetime.now())
    print("")

    soup = BeautifulSoup(page.text, "lxml")

    print("souped")
    print(datetime.datetime.now())
    print("")

    price = soup.find("span", attrs={'id': 'price_inside_buybox'}).string.strip()
    title = soup.find("span", attrs={'id': 'productTitle'}).string.strip()
    # rating = "" #soup.find("span", attrs={'class': 'a-icon-alt'}).string.strip()
    # imgUrl = "" #soup.find(id="imgTagWrapperId").findChild("img")['data-a-dynamic-image'].split(',')[0].split('"')[1]
    # imgUrl = img.findChild("img")['data-a-dynamic-image'].split(',')[0].split('"')[1]

    return price, title  # , rating, imgUrl


def parseUrl(url):
    productPrefix = url.find('/dp/')
    productStart = productPrefix + 4
    productEnd = url.find('/', productStart)
    productId = url[productStart:productEnd]
    finalUrl = 'https://www.amazon.com/dp/' + productId

    return finalUrl

