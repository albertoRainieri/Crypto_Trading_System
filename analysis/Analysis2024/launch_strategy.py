from Helpers import get_timeseries
from Helpers import get_analysis
from time import time

info_strategy_list_legend = {
        'strategy_jump': [0.04], #jump from price levels in terms of cumulative volume order (from 0 to 1)
        'limit': [0.25], #get the window of price change (from 0 to 1) (e.g. 0.15 check only the orders whose level is within 15% price change from current price)
        'price_change_jump': [0.025], #range of price change (used in get_price_levels for bid/ask_order_distribution)
        'max_limit': [0.1],  #what is the max change, beyond that I won't consider triggers ---> ( max_price - initial_price ) / initial_price
        'price_drop_limit': [0.1,0.05], #current drop from max. The minimum drop from the maximum for triggering events ---> abs( (current_price - max_price) / max_price )
        'buy_duration': [12],
        'stop_loss_limit': [0.05,"None"],
        'distance_jump_to_current_price': [0.01, 0.05], # minimum distance from jump
        'max_ask_od_level': [0.05,0.1], # max percentage of ask volume at the first level (level is based on price_change_jump)
        'last_i_ask_od': [1,3], # how many last order distribution obs I consider, an np.mean is executed
        'min_n_obs_jump_level': [1,5],
        'lvl': [1,3] #n_orderbook_levels
    }

def get_info_strategy(info_strategy_list_legend):
    info_strategy_list = []
    for strategy_jump in info_strategy_list_legend['strategy_jump']:
        for limit in info_strategy_list_legend['limit']:
            for price_change_jump in info_strategy_list_legend['price_change_jump']:
                for max_limit in info_strategy_list_legend['max_limit']:
                    for price_drop_limit in info_strategy_list_legend['price_drop_limit']:
                        for buy_duration in info_strategy_list_legend['buy_duration']:
                            for stop_loss_limit in info_strategy_list_legend['stop_loss_limit']:
                                for distance_jump_to_current_price in info_strategy_list_legend['distance_jump_to_current_price']:
                                    for max_ask_od_level in info_strategy_list_legend['max_ask_od_level']:
                                        for last_i_ask_od in info_strategy_list_legend['last_i_ask_od']:
                                            for min_n_obs_jump_level in info_strategy_list_legend['min_n_obs_jump_level']:
                                                for lvl in info_strategy_list_legend['lvl']:
                                                    info_strategy = {
                                                        'strategy_jump': strategy_jump,
                                                        'limit': limit,
                                                        'price_change_jump': price_change_jump,
                                                        'max_limit': max_limit,
                                                        'price_drop_limit': price_drop_limit,
                                                        'buy_duration': buy_duration,
                                                        'stop_loss_limit': stop_loss_limit,
                                                        'distance_jump_to_current_price': distance_jump_to_current_price,
                                                        'max_ask_od_level': max_ask_od_level,
                                                        'last_i_ask_od': last_i_ask_od,
                                                        'min_n_obs_jump_level': min_n_obs_jump_level,
                                                        'lvl': lvl
                                                    }
                                                    info_strategy_list.append(info_strategy)
    return info_strategy_list                 


if __name__ == '__main__':
    
    output, complete_info = get_analysis()
    event_keys_filter = []
    check_past=180 #minutes before event trigger
    check_future=1440 #minutes after the end of event (usually after 1 days from event trigger)

    save_plot=False
    analyze=True
    from multiprocessing import freeze_support
    freeze_support()  # Optional unless you’re freezing the app (e.g., with PyInstaller)
    info_strategy_list = get_info_strategy(info_strategy_list_legend)
    n_strategies = len(info_strategy_list)
    for info_strategy_i, info_strategy in enumerate(info_strategy_list):
        print(f"Strategy {info_strategy_i}/{n_strategies} started")
        t1 = time()
        get_timeseries(complete_info, check_past=check_past, check_future=check_future, info_strategy=info_strategy, save_plot=save_plot, analyze=analyze, n_processes=6, skip_if_strategy_exists=True)
        t2 = time()
        seconds_spent = t2 - t1
        minutes_spent = seconds_spent // 60
        print(f"Time taken: {minutes_spent} minutes")