import boto3
from datetime import datetime
import uuid
from decimal import *


def lambda_handler(event, context):
    #only needed if we move checkout to sudo app
    print(event)