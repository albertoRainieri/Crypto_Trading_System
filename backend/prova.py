import requests
URL="api.crypto.com"
method="public/get-book"
path=f"https://{URL}/v2/{method}?instrument_name=BTC_USDT&depth=10"
print(path)

response = requests.get(path)
print(response.text)