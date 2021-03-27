import logging
import sys
from time import gmtime

logging.Formatter.converter = gmtime
logging.basicConfig(format='%(asctime)s %(levelname)-8s [%(name)s.%(filename)s:%(lineno)d] %(message)s',
                    stream=sys.stdout,
                    level=logging.INFO)


def get(name=None):
    return logging.getLogger('sudocoins' if name is None else f'sudocoins.{name}')
