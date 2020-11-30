import requests
from bs4 import BeautifulSoup


def lambda_handler(event, context):

    url = event['url']

    cleanUrl = parseUrl(url)

    price, title, rating, imgUrl = loadAmazon(cleanUrl)

    return {
        'statusCode': 200,
        'body': {
            "price": price,
            "title": title,
            "rating": rating,
            "imgUrl": imgUrl
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
    soup = BeautifulSoup(page.content, "lxml")
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

