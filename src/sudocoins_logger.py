import logging
import sys
import os

print('initializing default logging config')
local_log_format = '%(asctime)s %(levelname)-5s [%(name)s.%(filename)s:%(lineno)d] %(message)s'
aws_log_format = '%(asctime)s %(aws_request_id)s %(levelname)-5s [%(name)s.%(filename)s:%(lineno)d] %(message)s'

# init local logging: does nothing if the root logger already has handlers configured
logging.basicConfig(format=local_log_format, stream=sys.stdout)
# init aws logging
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
    root_logger.handlers[0].setFormatter(logging.Formatter(aws_log_format))


def get(name=None, level=logging.DEBUG):
    logger = logging.getLogger('sudocoins' if name is None else f'sudocoins.{name}')
    logger.setLevel(level)
    return logger
