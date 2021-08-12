import os
from requests_oauthlib import OAuth1Session
from util import sudocoins_logger

log = sudocoins_logger.get()

CONSUMER_KEY = os.environ.get("CONSUMER_KEY")
CONSUMER_SECRET = os.environ.get("CONSUMER_SECRET")
TWITTER_REQUEST_TOKEN_URL = os.environ.get("TWITTER_REQUEST_TOKEN_URL")

def lambda_handler(event, context):
    set_log_context(event)
    request_token = OAuth1Session(
        client_key=CONSUMER_KEY, client_secret=CONSUMER_SECRET, callback_uri="http://localhost:3000/following"
    )
    data = request_token.get(TWITTER_REQUEST_TOKEN_URL)
    if data.status_code == 200:
        request_token = str.split(data.text, '&')
        oauth_token = str.split(request_token[0], '=')[1]
        oauth_callback_confirmed = str.split(request_token[2], '=')[1]
        return {
            "oauth_token": oauth_token,
            "oauth_callback_confirmed": oauth_callback_confirmed
        }
    else:
        return {
            "oauth_token": None,
            "oauth_callback_confirmed": "false"
        }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))