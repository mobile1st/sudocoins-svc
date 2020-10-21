import json
import os
import boto3


def lambda_handler(event, context):
    response = {}
    response["statusCode"]=302
    response["headers"]={'Location': 'https://master.d2wa1oa4l29mvk.amplifyapp.com/'}
    data = {}
    response["body"]=json.dumps(data)

    try:
        params = event["queryStringParameters"]
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName='EndTransaction.fifo')
        item = {
            "transaction_id": params["transaction_id"],
            "status": params["status"],
            "IP address": params["IP address"],
            "transaction_timestamp": params["transaction_timestamp"],
            "signature_hmac_sha": params["signature_hmac_sha"]}
        try:
            record = queue.send_message(MessageBody=json.dumps(item), MessageGroupId='cint')
            return response #add query parameter data based on transaction result

        except Exception as e:
            return response #add qp data based on error

    except Exception as e:
        return response #add qp data
        print(e)


