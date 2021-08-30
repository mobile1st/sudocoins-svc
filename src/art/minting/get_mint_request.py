import boto3
from util import sudocoins_logger
import json
import requests

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    body = json.loads(event['body'])

    art_id = body['art_id']

    name = body['name']
    description = body['description']
    ipfs_image = body['ifps']

    creator = body['creator']
    royalty = body['royalty']

    #  sale_price = body['sale_price']

    token_id = get_token_id(creator)

    ipfs_meta = generate_meta_data(name, description, ipfs_image, art_id)

    mint_request = get_mint_request(token_id, ipfs_meta, royalty, creator)

    return mint_request


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def generate_meta_data(name, description, image, art_id):
    data = {
        "name": name,
        "description": description,
        "image": image,
        "external_url": "https://app.sudocoins.com/art/social/" + art_id
    }

    with open('/tmp/metadata.json', 'w') as outfile:
        json.dump(data, outfile)

    files = {
        'file': '/tmp/metadata.json'
    }
    response = requests.post('https://ipfs.infura.io:5001/api/v0/add', files=files,
                             auth=('1xBSq6KuqrbDmhs2ASr722Cs8JF', '3c98ebb9f76fabc45d519718e41dd4f0'))

    ipfs_meta_hash = json.loads(response.text)

    return ipfs_meta_hash['Hash']


def get_token_id(creator):
    token_id_endpoint = "https://api-dev.rarible.com/protocol/v0.1/ethereum/nft/collections/" \
                        "0xB0EA149212Eb707a1E5FC1D2d3fD318a8d94cf05/generate_token_id?minter="
    request_url = token_id_endpoint + creator
    response = requests.get(request_url)
    response_parsed = json.loads(response.text)
    token_id = response_parsed['tokenId']

    return token_id


def get_mint_request(tokenId, ipfs_hash, royalty, creator):
    request = {
        "types": {
            "EIP712Domain": [
                {
                    "type": "string",
                    "name": "name"
                },
                {
                    "type": "string",
                    "name": "version"
                },
                {
                    "type": "uint256",
                    "name": "chainId"
                },
                {
                    "type": "address",
                    "name": "verifyingContract"
                }
            ],
            "Mint721": [
                {"name": "tokenId", type: "uint256"},
                {"name": "tokenURI", type: "string"},
                {"name": "creators", type: "Part[]"},
                {"name": "royalties", type: "Part[]"}
            ],
            "Part": [
                {"name": "account", type: "address"},
                {"name": "value", type: "uint96"}
            ]
        },
        "domain": {
            "name": "Mint721",
            "version": "1",
            "chainId": 3,
            "verifyingContract": "0xB0EA149212Eb707a1E5FC1D2d3fD318a8d94cf05"
        },
        "primaryType": "Mint721",
        "message": {
            "@type": "ERC721",
            "contract": "0xB0EA149212Eb707a1E5FC1D2d3fD318a8d94cf05",
            "tokenId": tokenId,
            "uri": "/ipfs/" + ipfs_hash,
            "creators": [
                {
                    "account": creator,
                    "value": "10000"}
            ],
            "royalties": [
                {
                    "account": "0x883a77493C18217D38149139951815A4d8d721cA",
                    "value": 200
                },
                {
                    "account": creator,
                    "value": royalty
                }
            ],
        }
    }

    return request

