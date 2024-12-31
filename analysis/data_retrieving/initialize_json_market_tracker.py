import json
from datetime import datetime

# paths saved in the USB DRIVE
last_tracker_saved = '/Volumes/PortableSSD/Alberto/Trading/json_tracker/data-2024-08-15-00-0.json'
last_market_saved = '/Volumes/PortableSSD/Alberto/Trading/json_market/data-2024-08-12-12-0.json'
path_project = '/Users/albertorainieri/Personal'


paths = {'json_tracker': last_tracker_saved, 'json_market': last_market_saved}
CHECK_OK = {'json_tracker': True, 'json_market': True}

last_timestamp_doc = {}


for path in paths:
    print(f'Analyzing {path}')
    with open(paths[path], 'r') as f:
        data = json.load(f)

    last_timestamp_saved_btc = data['data']['BTCUSDT'][-1]['_id']
    last_datetime_saved_btc = datetime.fromisoformat(last_timestamp_saved_btc).replace(second=0, microsecond=0)
    last_timestamp_doc[path] = last_timestamp_saved_btc

    for coin in data['data']:
        if len(data['data'][coin]) > 0:
            last_obs = data['data'][coin][-1]
            if datetime.fromisoformat(last_obs['_id']).replace(second=0, microsecond=0) != last_datetime_saved_btc:
                last_ts = last_obs['_id']
                print(f'WARNING - BTCUSDT: {last_timestamp_saved_btc} vs {coin} : {last_ts} has a different timestamp saved:')
                CHECK_OK[path] = False

    if CHECK_OK[path]:
        print('Perfect! All last obs for each coin  are saved at the same time')

for path in last_timestamp_doc:
    last_ts = last_timestamp_doc[path]
    print(f'Last coin saved for {path} in USB Drive is {last_ts}')


answer = input('\n Do you want to initialize a new json in analysis/json_market and analysis/json_tracker. \n The start time (i.e. datetime_creation) is based on these previous timestamps for both tracker and market \n (yes/no) \n')


if answer == 'yes':
    for path in last_timestamp_doc:

        my_dict = {
            'datetime_creation': last_timestamp_doc[path],
            'data': {}
        }

        # Save as a JSON string
        json_string = json.dumps(my_dict, indent=4)
        year = datetime.fromisoformat(last_timestamp_doc[path]).year
        month = datetime.fromisoformat(last_timestamp_doc[path]).month
        day = datetime.fromisoformat(last_timestamp_doc[path]).day
        hour = datetime.fromisoformat(last_timestamp_doc[path]).hour
        minute = datetime.fromisoformat(last_timestamp_doc[path]).minute
        new_json_path = f'{path_project}/analysis/{path}/data-{year}-{month}-{day}-{hour}-{minute}.json'

        print(f'Initializing {new_json_path} for {path}')
        print(json_string)

        # Save as a JSON file
        # with open(new_json_path, 'w') as outfile:
        #     json.dump(my_dict, outfile, indent=4)