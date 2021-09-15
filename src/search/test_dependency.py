import requests
import pyvips


def lambda_handler(event, context):
    print(requests.get('https://google.com'))
    image = pyvips.Image.new_from_file('test.svg')
    image.write_to_file('test.png')
