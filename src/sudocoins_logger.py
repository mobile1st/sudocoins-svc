import logging
import sys

print('initializing default logging config')
# init local logging: does nothing if the root logger already has handlers configured
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s [%(name)s.%(filename)s:%(lineno)d] %(message)s',
    stream=sys.stdout
)
# init aws logging
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)


def get(name=None, level=logging.DEBUG):
    logger = logging.getLogger('sudocoins' if name is None else f'sudocoins.{name}')
    logger.setLevel(level)
    return logger
