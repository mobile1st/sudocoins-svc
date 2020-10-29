import json
import os
import boto3
import hashlib
import base64


def lambda_handler(event, context):
    redirectUrl = 'https://master.d2wa1oa4l29mvk.amplifyapp.com/'
    msg = '?msg='
    data = {}
    params = event["queryStringParameters"]

    # Create sqs message and redirect message
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
        kmsClient = boto3.client('kms')
        try:
            record = queue.send_message(MessageBody=json.dumps(msgValue), MessageGroupId='EndTransaction')
            secret = json.dumps(msgValue, indent=2).encode('utf-8')
            token = encrypt(kmsClient, secret, os.environ["keyId"])
            response = {
                "statusCode": 302,
                "headers": {'Location': redirectUrl + msg + hs + '&status=' + params["c"] + "&token=" + token},
                "body": json.dumps(data)
            }

            return response

        except Exception as e:
            print(e)
            msgValue["error"] = "invalid_transaction_id"
            secret = json.dumps(msgValue, indent=2).encode('utf-8')
            token = encrypt(kmsClient, secret, os.environ["keyId"])
            response = {
                "statusCode": 302,
                "headers": {'Location': redirectUrl + msg + msgValue["error"] + "&token=" + token},
                # . add encrypted token
                "body": json.dumps(data)

            }

            return response

    except Exception as e:
        msgValue["error"] = "error"
        secret = json.dumps(msgValue, indent=2).encode('utf-8')
        token = encrypt(kmsClient, secret, os.environ["keyId"])
        response = {
            "statusCode": 302,
            "headers": {'Location': redirectUrl + msg + msgValue["error"] + "&token=" + token},  # . add encrypted token
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


def encrypt(client, secret, keyId):
    ciphertext = client.encrypt(
        KeyId=keyId,
        Plaintext=bytes(secret),
    )
    token = base64.b64encode(ciphertext["CiphertextBlob"]).decode("utf-8")

    return token


'''
this function will decrypt
def decrypt(secret, keyId):
    client = boto3.client('kms')
    dec = base64.b64decode(secret)
    plaintext = client.decrypt(
        CiphertextBlob=dec, KeyId=keyId)

    return plaintext["Plaintext"]

'''