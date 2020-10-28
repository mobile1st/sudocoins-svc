import json
import os
import boto3
import hashlib


# . from cryptography.fernet import Fernet


def lambda_handler(event, context):
    redirectUrl = 'https://master.d2wa1oa4l29mvk.amplifyapp.com/'
    msg = '?msg='
    data = {}
    params = event["queryStringParameters"]
    # . f = Fernet("MY_KEY")

    # create sqs message and redirect message
    msgValue = {}
    msgValue["referer"] = event["headers"]["referer"]
    msgValue["queryStringParameters"] = params

    # Verify Hash
    sha = params["h"]
    url = ("https://1ql2u7kixc.execute-api.us-west-2.amazonaws.com/prod/SudoCoinsSurveyEnd?"
           "ts={0}&t={1}&c={2}&ip={3}").format(params["ts"], params["t"], params["c"], params["ip"])
    hashState = checkSha(url, sha)
    if hashState == True:
        msgValue["hashState"] = True
        hs = "True"
    else:
        msgValue["hashState"] = False
        hs = "False"

    # Missing params
    expectedParams = ["c", "h", "t", "ts", "ip"]
    receivedParams = params.keys()
    missingParams = []
    for i in expectedParams:
        if i not in receivedParams:
            missingParams.append(i)
    msgValue["missingParams"] = missingParams

    try:
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName='EndTransaction.fifo')
        print("try")
        try:
            record = queue.send_message(MessageBody=json.dumps(msgValue), MessageGroupId='EndTransaction')
            encodeData = json.dumps(msgValue, indent=2).encode('utf-8')
            # . token = f.encrypt(encodeData).decode(encoding='UTF-8')
            response = {
                "statusCode": 302,
                "headers": {'Location': redirectUrl + msg + hs + '&status=' + params["c"]},  # . add encrypted token
                "body": json.dumps(data)

            }

            return response

        except Exception as e:
            msgValue["error"] = "invalid_transaction_id"
            encodeData = json.dumps(msgValue, indent=2).encode('utf-8')
            # . token = f.encrypt(encodeData).decode(encoding='UTF-8')
            response = {
                "statusCode": 302,
                "headers": {'Location': redirectUrl + msg + msgValue["error"]},  # . add encrypted token
                "body": json.dumps(data)

            }

            return response

    except Exception as e:
        msgValue["error"] = "error"
        encodeData = json.dumps(msgValue, indent=2).encode('utf-8')
        # . token = f.encrypt(encodeData).decode(encoding='UTF-8')
        response = {
            "statusCode": 302,
            "headers": {'Location': redirectUrl + msg + msgValue["error"]},  # . add encrypted token
            "body": json.dumps(data)
        }

        return response


def checkSha(url, hash):
    hashObject = hashlib.sha256(url.encode('utf-8'))
    hexDig = hashObject.hexdigest()
    if hexDig == hash:
        return True
    else:
        return False

