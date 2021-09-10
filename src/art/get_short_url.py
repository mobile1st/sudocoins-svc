import base64
import uuid
from util import sudocoins_logger

log = sudocoins_logger.get()


def lambda_handler(event, context):
    set_log_context(event)
    art_id = event['pathParameters']['shareId']
    short_id = encode_uid(art_id)
    return {
        'shortUrl': f'https://sudo.art/{short_id}'
    }


def encode_uid(uid):
    uid_obj = uuid.UUID(f'{uid}')
    b64 = base64.urlsafe_b64encode(uid_obj.bytes).rstrip(bytes('=', 'utf-8'))
    return b64.decode('utf-8')


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))
