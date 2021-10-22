from copy import Error
import pandas as pd
import more_itertools
from .enums import *
from math import log
from decimal import Decimal

def calculate_fee(amount, fee, digit=8):
    return round(safe_multiply(amount,fee), digit)

def time_scale_to_minute(interval: str):
    seconds_per_unit = {
        "m": 1,
        "h": 60,
        "d": 24 * 60,
        "w": 7 * 24 * 60,
    }
    try:
        return int(interval[:-1]) * seconds_per_unit[interval[-1]]
    except (ValueError, KeyError):
        return None

def round_step_downward(quantity, step_size):
    if step_size == 0: raise Exception('step_size cannot be 0!')
    precision: int = int(round(-log(step_size, 10), 0))
    return truncate(quantity,precision)

def truncate(num,n):
    temp = str(num)
    for x in range(len(temp)):
        if temp[x] == '.':
            try:
                return float(temp[:x+n+1])
            except:
                return float(temp)      
    return float(temp)

def safe_multiply(num1, num2):
    return float( (Decimal(str(num1)) * Decimal(str(num2))).quantize(Decimal('0.00000001'), rounding=None) )

def safe_sum(num1, num2):
    return float( (Decimal(str(num1)) + Decimal(str(num2))).quantize(Decimal('0.00000001'), rounding=None) )

def safe_substract(num1, num2):
    return float( (Decimal(str(num1)) - Decimal(str(num2))).quantize(Decimal('0.00000001'), rounding=None) )

def time_scale_to_second(interval: str):
    return time_scale_to_minute(interval) * 60

def time_scale_to_milisecond(interval: str):
    return time_scale_to_minute(interval) * 60 * 1000

def eval_total_capital(df_balance, lto_list, quote_currency, max_capital_use_ratio=1):
    # Toal capital: Free QC + LTO_enter
    free_qc = df_balance.loc[quote_currency,'free']

    # NOTE: In-trade balance is calculated only by considering the LTOs of the Ikarus
    #       Using only the df_balance requires live updates and evaluation of each asset in terms of QC

    # NOTE: If state of a TO is:
    #   'closed': then the amount that is used by this TO is reflected back to main capital (df_balance in backtest (by lto_update))
    #           : these LTOs needs be omitted
    #   'enter_expire': then it is marked to be handled by the their strategy but the balance is still locked in LTO

    in_trade_qc = eval_total_capital_in_lto(lto_list)

    total_qc = safe_sum(free_qc, in_trade_qc)
    return safe_multiply(total_qc, max_capital_use_ratio)

def eval_total_capital_in_lto(lto_list):
    in_trade_qc = 0
    for lto in lto_list:
        # Omit the LTOs that are closed, because their use of amount returned to df_balance (by broker or by lto_update of test-engine)
        if lto['status'] != STAT_CLOSED:
            # NOTE: It is assumed that each object may only have 1 TYPE of exit or enter:
            # TODO: NEXT: CAREFULL: The approach above make sense but the action logic may needs to be changed in case of market_exit:
            #       As a solution, when market exit decision is made. Simply save the old oco or limit, then remove it to make sure that there is only one
            in_trade_qc = safe_sum(in_trade_qc, more_itertools.one(lto[PHASE_ENTER].values())['amount'])
    return in_trade_qc

async def get_closed_hto(config, mongocli, query={'result.cause':STAT_CLOSED}):
    # TODO: NEXT: All statistics needs to be changed a bit  to integrate market orders
    # Read Database to get hist-trades and dump to a DataFrame
    hto_list = await mongocli.do_find('hist-trades',query)
    hto_closed = []
    for hto in hto_list:

        # Ideal enter and exit types
        enter_type = config['strategy'][hto['strategy']]['enter']['type']
        exit_type = config['strategy'][hto['strategy']]['exit']['type']
        # NOTE: These ideal types only change in market_exit action which is handled in get_exit_expire_hto

        if exit_type == TYPE_OCO: plannedPriceName = 'limitPrice'
        elif exit_type == TYPE_LIMIT or exit_type == TYPE_MARKET: plannedPriceName = 'price'

        hto_dict = {
            "_id": hto['_id'],
            "strategy": hto['strategy'],
            "decision_time": hto['decision_time'],
            "enterTime": hto['result']['enter']['time'],
            "enterPrice": hto['enter'][enter_type]['price'], # TODO: NEXT: TYPE_LIMIT 
            "exitTime": hto['result']['exit']['time'],
            "exitPrice": hto['exit'][exit_type][plannedPriceName],
            "sellPrice": hto['result']['exit']['price']
        }
        hto_closed.append(hto_dict)

    df = pd.DataFrame(hto_closed)
    return df


async def get_enter_expire_hto(mongocli, query={'result.cause':STAT_ENTER_EXP}):
    # Read Database to get hist-trades and dump to a DataFrame
    hto_list = await mongocli.do_find('hist-trades',query)
    hto_ent_exp_list = []
    for hto in hto_list:
        # NOTE: HIGH: We dont know it the exit type is limit or not
        hto_dict = {
            "_id": hto['_id'],
            "strategy": hto['strategy'],
            "decision_time": hto['decision_time'],
            "enterExpire": hto['enter'][TYPE_LIMIT]['expire'],# TODO: NEXT: TYPE_LIMIT | TODO: use result enter price
            "enterPrice": hto['enter'][TYPE_LIMIT]['price'],
        }
        hto_ent_exp_list.append(hto_dict)

    df = pd.DataFrame(hto_ent_exp_list)
    return df


async def get_exit_expire_hto(config, mongocli, query={'result.cause':STAT_EXIT_EXP}):
    # Read Database to get hist-trades and dump to a DataFrame
    
    hto_list = await mongocli.do_find('hist-trades',query)
    hto_closed_list = []
    for hto in hto_list:

        enter_type = config['strategy'][hto['strategy']]['enter']['type']
        exit_type = config['strategy'][hto['strategy']]['exit']['type']

        if exit_type == TYPE_OCO: plannedPriceName = 'limitPrice'
        elif exit_type == TYPE_LIMIT: plannedPriceName = 'price'

        # Initial (ideal) exit module is saved to update_history list
        initial_exit_module = hto['update_history'][0]
        # TODO: Rename the update_history with some proper name

        hto_dict = {
            "_id": hto['_id'],
            "strategy": hto['strategy'],
            "decision_time": hto['decision_time'],
            "enterTime": hto['result']['enter']['time'],
            "enterPrice": hto['enter'][enter_type]['price'],            # Ideally enter limit orders are executed with the exact prices
            "exitPrice": initial_exit_module[plannedPriceName],
            "sellPrice": hto['result']['exit']['price'],
            "exitExpire": initial_exit_module['expire']
        }
        hto_closed_list.append(hto_dict)
    df = pd.DataFrame(hto_closed_list)

    return df


async def get_pair_min_period_mapping(config):
    
    pair_scale_set_mapping = {}
    for name, strategy in config['strategy'].items():
        for pair in strategy['pairs']:
            if pair not in pair_scale_set_mapping.keys():
                pair_scale_set_mapping[pair] = set()
            pair_scale_set_mapping[pair].add(strategy['time_scales'][0])

    pair_min_period_mapping = {}
    for pair, scale_set in pair_scale_set_mapping.items():
        for scale in config['time_scales'].keys():
            if scale in scale_set:
                pair_min_period_mapping[pair] = scale
                break

    return pair_min_period_mapping


async def get_min_scale(ordered_scales, pool):
    
    for scale in ordered_scales:
        if scale in pool:
            return scale

    return None

def round_to_period(current_time, period_min, direction='floor', offset=0):
    """
    Args:
        current_time ([type]): [description]
        period_min ([type]): [description]
        direction (str, optional): [floor|ceiling]. Defaults to 'floor'.
        offset (int, optional): [description]. Defaults to 0.

    Returns:
        [int]: [rounded_time]
    """    
    current_time -= (current_time % 60)                         # exact minute
    current_time -= (current_time % period_min )                # exact scale
    if direction == 'floor':
        rounded_time = current_time + offset
    elif direction == 'ceiling':
        rounded_time = current_time + period_min + offset
    else:
        raise Exception(f'Unknown direction: {direction}')
    return int(rounded_time)