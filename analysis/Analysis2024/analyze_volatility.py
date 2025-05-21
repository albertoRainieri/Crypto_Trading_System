# QUESTO FILE ANALIZZA IN MODO PIù GENERALE GLI ORDINI CON VOLUMI ALTI

import os,sys
sys.path.insert(0, "..")
sys.path.insert(0, "../..")
from Functions import total_function_multiprocessing
from Helpers import create_event_keys
from time import sleep
from datetime import datetime
import json 
from multiprocessing import freeze_support
# if True, the analysis starts from the current event-keys used in production
# else, it starts an analysis based on the keys in "event_keys" folder
def main():
    KEEP_PRODUCTION_ANALYSIS = True

    if not KEEP_PRODUCTION_ANALYSIS:
        analysis_name = 'analysis-nobuyvolume-3'
        list_minutes = '1440'
        event_keys_path = "/Users/albertorainieri/Personal/analysis/Analysis2024/event_keys/event_keys.json"
        event_keys = create_event_keys(event_keys_path, list_minutes, analysis_name)
    else:
        analysis_name = None
        riskmanagement_json_prod_path = "/Users/albertorainieri/Personal/analysis/Analysis2024/analysis_json_production/riskmanagement.json"
        with open(riskmanagement_json_prod_path, 'r') as file:
            riskmanagement_json_prod = json.load(file)
            event_keys = list(riskmanagement_json_prod.keys())

    n_processes = 8
    analysis_timeframe=7

    UNLOCK = True
    n_iterations = 10

    for i in range(n_iterations):
        if UNLOCK:
            shared_data = total_function_multiprocessing(event_keys, analysis_timeframe, n_processes, KEEP_PRODUCTION_ANALYSIS, analysis_name=analysis_name, start_interval=datetime(2024, 1, 1).isoformat()) 
            iterazione = i+1
        msg=f'{iterazione}/{n_iterations} COMPLETED'
        print(msg)

        print('sleeping')
        sleep(5)

if __name__ == "__main__":
    freeze_support()
    main()