import json
from datetime import datetime
# paths saved in the USB DRIVE
path = '/Users/albertorainieri/Personal/analysis/json_tracker/data-2024-02-04-00-00.json'
new_path = '/Users/albertorainieri/Personal/analysis/json_tracker/data-2024-02-04-00-01.json'
path_project = '/Users/albertorainieri/Personal'

datetime_limit = datetime(2024, 2, 7, 6, 25)

print(f'Analyzing {path}')
with open(path, 'r') as f:
    data = json.load(f)
    new_data = {'datetime_creation': datetime_limit.isoformat(),
                 'data': {}}

for coin in data['data']:
    delete = 0
    for obs in data['data'][coin]:
        if datetime.fromisoformat(obs['_id']) > datetime_limit:
            if coin not in new_data['data']:
                new_data['data'][coin] = []
            
            new_data['data'][coin].append(obs)
        else:
            delete += 1
    print(f'{coin}: {delete} obs have been deleted')

with open(new_path, "w") as f:
    json.dump(new_data, f, indent=4)
