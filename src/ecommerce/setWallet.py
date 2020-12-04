import boto3
from datetime import datetime
import uuid
from decimal import *


def lambda_handler(event, context):
    print(event)