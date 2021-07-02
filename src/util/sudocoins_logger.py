import logging
import sys
from util import request_util
from time import gmtime

root_log_format = '[%(asctime)s] %(levelname)-5s [%(name)s.%(filename)s:%(lineno)d] %(message)s'
sc_log_format = '[%(asctime)s] %(levelname)-5s [ip:%(ip)s][sub:%(sub)s][%(name)s.%(filename)s:%(lineno)d] %(message)s'

logging.basicConfig(stream=sys.stdout)
logging.Formatter.converter = gmtime

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.handlers[0].setFormatter(logging.Formatter(root_log_format))

sudocoins_logger = logging.getLogger('sc')
ch = logging.StreamHandler(stream=sys.stdout)
ch.setFormatter(logging.Formatter(sc_log_format))
sudocoins_logger.addHandler(ch)
sudocoins_logger.setLevel(logging.DEBUG)
sudocoins_logger.propagate = False
unknown = 'N/A'


def get(ctx=None):
    extra = ctx or get_ctx_dict()
    return logging.LoggerAdapter(sudocoins_logger, extra=extra)


def get_ctx(event):
    return get_ctx_dict(request_util.get_ip_address_safe(event), request_util.get_sub_safe(event))


def get_ctx_dict(ip=None, sub=None):
    return {'ip': ip or unknown, 'sub': sub or unknown}
