import base64
import uuid


def encode_uid(uid):
    uid_obj = uuid.UUID(f'{uid}')
    b64 = base64.urlsafe_b64encode(uid_obj.bytes).rstrip(bytes('=', 'utf-8'))
    return b64.decode('utf-8')


def decode_uid(b64: str):
    padded = b64 + "=" * (-len(b64) % 4)
    dec = base64.urlsafe_b64decode(padded)
    return uuid.UUID(bytes=dec)
