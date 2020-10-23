import json
import os
import boto3
import hashlib


def lambda_handler(event, context):
    #  Redirect to Profile page
    redirect_URL = 'https://master.d2wa1oa4l29mvk.amplifyapp.com/'
    msg = '?msg='
    data = {}
    '''
    #Verify Hash
    params = event["queryStringParameters"]
    signature_hmac_sha = params["signature_hmac_sha"]
    URL = "grab from event or re-create"
    hashState = checkSha(URL, signature_hmac_sha)

    #Missing params
    expectedParams = ["transaction_id", "status", "IP address",
                      "transaction_timestamp","signature_hmac_sha"]
    receivedParams = params.keys()
    missingParams = []
    for i in expectedParams:
        if i not in receivedParams:
            missingParams.append(i)

    #add hashState and MissingParams to the item
    #convert item to json and encrypt
    #append hash to redirect
    '''
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
            msg = msg + str(params["status"])
            record = queue.send_message(MessageBody=json.dumps(item), MessageGroupId='EndTransaction')
            response = {"statusCode": 302, "headers": {'Location': redirect_URL + msg}}
            response["body"] = json.dumps(data)
            return response

        except Exception as e:
            msg = msg + "invalid_transaction_id"
            response = {"statusCode": 302, "headers": {'Location': redirect_URL + msg}}
            response["body"] = json.dumps(data)
            return response

    except Exception as e:
        msg = msg + "error"
        response = {"statusCode": 302, "headers": {'Location': redirect_URL + msg}}
        response["body"] = json.dumps(data)
        return response

    #  see if sha256 hash of redirect URL matches hash from Buyer


'''
def checkSha(URL,signature_hmac_sha):
    hash_object = hashlib.sha256(URL.encode('utf-8'))
    hex_dig = hash_object.hexdigest()
    if hex_dig == signature_hmac_sha:
        return True
    else:
        return False
'''
