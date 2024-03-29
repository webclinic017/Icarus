import asyncio

from pymongo import ASCENDING, DESCENDING
from .. import mongo_utils, binance_wrapper
#from scripts import finplot_wrapper as fplot
from . import finplot_wrapper as fplot
from ..enums import *
from ..utils import get_closed_hto, get_enter_expire_hto, get_exit_expire_hto, get_pair_min_period_mapping
from binance import AsyncClient
import pandas as pd
import json
import sys
from datetime import datetime, timezone

async def visualize_online(bwrapper, mongocli, config):

    start_time = datetime.strptime(str(sys.argv[2]), "%Y-%m-%d %H:%M:%S")
    #start_timestamp = int(datetime.timestamp(start_time))*1000
    start_timestamp = int(start_time.replace(tzinfo=timezone.utc).timestamp())*1000
    end_time = datetime.strptime(str(sys.argv[3]), "%Y-%m-%d %H:%M:%S")
    #end_timestamp = int(datetime.timestamp(end_time))*1000
    end_timestamp = int(end_time.replace(tzinfo=timezone.utc).timestamp())*1000

    pair_scale_mapping = await get_pair_min_period_mapping(config)

    df_list = []
    for pair,scale in pair_scale_mapping.items(): 
        df_list.append(bwrapper.get_historical_klines(start_timestamp, end_timestamp, pair, scale))

    df_pair_list = list(await asyncio.gather(*df_list))

    for idx, pair in enumerate(pair_scale_mapping.keys()):
        df_enter_expire = await get_enter_expire_hto(mongocli, {
            'result.cause':STAT_ENTER_EXP, 
            'pair':pair, 
            'decision_time': { '$gte': start_timestamp}, 
            'enter.limit.expire': { '$lte': end_timestamp}
            })
        df_exit_expire = await get_exit_expire_hto(config, mongocli, {
            'result.cause':STAT_EXIT_EXP, 
            'pair':pair, 
            'decision_time': { '$gte': start_timestamp}, 
            'result.exit.time': { '$lte': end_timestamp}
            })
        df_closed = await get_closed_hto(config, mongocli, {
            'result.cause':STAT_CLOSED, 
            'pair':pair, 
            'decision_time': { '$gte': start_timestamp}, 
            'result.exit.time': { '$lte': end_timestamp}
            })

        fplot.buy_sell(df_pair_list[idx], df_closed=df_closed, df_enter_expire=df_enter_expire, df_exit_expire=df_exit_expire)

    pass


async def visualize_dashboard(bwrapper, mongocli, config):

    start_obs = await mongocli.get_n_docs('observer', {'type':'qc'}, order=ASCENDING) # pymongo.ASCENDING
    end_obs = await mongocli.get_n_docs('observer', {'type':'qc'}, order=DESCENDING) # pymongo.ASCENDING

    pair_scale_mapping = await get_pair_min_period_mapping(config)

    df_list, dashboard_data_pack = [], {}
    for pair,scale in pair_scale_mapping.items(): 
        df_list.append(bwrapper.get_historical_klines(int(start_obs[0]['timestamp']), int(end_obs[0]['timestamp']), pair, scale))
        dashboard_data_pack[pair]={}
    
    df_pair_list = await asyncio.gather(*df_list)

    # Get trade objects
    for idx, item in enumerate(pair_scale_mapping.items()):
        # TODO: Optimize and clean the code: e.g. assign call outputs directly to the dataframes
        df_enter_expire = await get_enter_expire_hto(mongocli,{'result.cause':STAT_ENTER_EXP, 'pair':item[0]})
        df_exit_expire = await get_exit_expire_hto(config, mongocli, {'result.cause':STAT_EXIT_EXP, 'pair':item[0]})
        df_closed = await get_closed_hto(config, mongocli, {'result.cause':STAT_CLOSED, 'pair':item[0]})

        dashboard_data_pack[item[0]]['df'] = df_pair_list[idx]
        dashboard_data_pack[item[0]]['df_enter_expire'] = df_enter_expire
        dashboard_data_pack[item[0]]['df_exit_expire'] = df_exit_expire
        dashboard_data_pack[item[0]]['df_closed'] = df_closed

    # Get trade objects
    qc_list = list(await mongocli.do_find('observer',{'type':'qc'}))
    # TODO: NEXT: Fix the logic by looking at visualize_test_sessions.py
    df = pd.DataFrame([item['qc'] for item in qc_list], index=[item['timestamp'] for item in qc_list])
    dashboard_data_pack['qc'] = df
    fplot.buy_sell_dashboard(dashboard_data_pack=dashboard_data_pack, title=f'Visualizing Time Frame:')


async def main():

    client = await AsyncClient.create(api_key=cred_info['Binance']['Test']['PUBLIC-KEY'],
                                    api_secret=cred_info['Binance']['Test']['SECRET-KEY'])
    bwrapper = binance_wrapper.TestBinanceWrapper(client, config)
    mongocli = mongo_utils.MongoClient(config['mongodb']['host'], 
        config['mongodb']['port'], 
        config['tag'],
        clean=False)

    #await visualize_online(bwrapper, mongocli, config)
    await visualize_dashboard(bwrapper, mongocli, config)
    await bwrapper.client.close_connection()


if __name__ == '__main__':

    f = open(str(sys.argv[1]),'r')
    config = json.load(f)
    with open(config['credential_file'], 'r') as cred_file:
        cred_info = json.load(cred_file)
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())