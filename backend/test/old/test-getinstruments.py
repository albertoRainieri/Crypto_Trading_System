import os,sys
import httpx
sys.path.insert(0,'../..')

import asyncio
from binance import AsyncClient
from binance.client import Client
from backend.constants.constants import *
from operator import itemgetter


import json


async def get_async(url):
    #header = CryptoController.getHeader()
    async with httpx.AsyncClient() as client:
        return await client.get(url)


async def gather(coins_list, interval='1d', limit=1):
    urls = []

    for coin in coins_list:
        urls.append(BINANCE_ENDPOINT + 'klines?symbol=' + coin + '&interval=' + interval + '&limit=' + str(limit))

    
    resps = await asyncio.gather(*map(get_async, urls))
    data = [json.loads(resp.text) for resp in resps]

    return data


async def main1():
    client = await AsyncClient.create()

    # fetch exchange info
    res = await client.get_exchange_info()
    await client.close_connection()

    return res

def main2(res):
    #print('res', res)
    all_usdt_coins = []
    for coin in res['symbols']:
        if coin['symbol'][-4:] == 'USDT' and coin['symbol'] != 'USDCUSDT' and coin['symbol'] != 'TUSDUSDT':
            all_usdt_coins.append(coin['symbol'])
    
    number_coins = len(all_usdt_coins)
    print(f'Number of coins: {number_coins}')



    json_object = json.dumps(res, indent=2)
    dict_ = {'most_traded_coins': all_usdt_coins}
    with open("instruments.json", "w") as outfile:
        outfile.write(json.dumps(dict_))
    
    tries = 0
    try:
        if tries < 10:
            tries += 1
            trades = asyncio.run(gather(coins_list=all_usdt_coins))
    except:
        main2()


    list_volumes = []
    most_traded_coins_list = []

    for coin,trade in zip(all_usdt_coins, trades):
        
        dict_volume = {'coin': coin, 'volume': float(trade[0][7])}
        list_volumes.append(dict_volume)

    list_volumes.sort(key=lambda x: x['volume'], reverse=True)

    for obj in list_volumes:
        most_traded_coins_list.append(obj['coin'])


    sorted_volumes = {'most_traded_coins': list_volumes}
    with open("sorted_instruments.json", "w") as outfile_volume:
        outfile_volume.write(json.dumps(sorted_volumes))

    most_traded_coins = {'most_traded_coins': most_traded_coins_list}
    with open("most_traded_coins.json", "w") as outfile_volume:
        outfile_volume.write(json.dumps(most_traded_coins))
    

if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    res = loop.run_until_complete(main1())
    main2(res)