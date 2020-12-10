from coinbase_commerce.client import Client

API_KEY = "31105b04-ee3e-43c8-a256-3a2d7e818c9c"
client = Client(api_key=API_KEY)

print(client)


charge = client.charge.create(name='Amazon Gift Card',
                              description='Shop online using amazon.com',
                              pricing_type='fixed_price',
                              local_price={
                                  "amount": "100.00",
                                  "currency": "USD"
                              },
                              metadata= {
                                "customer_id": "222",
                                "customer_name": "Satoshi Nakamoto"
                                })

print(charge)