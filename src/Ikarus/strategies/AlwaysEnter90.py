import logging
import statistics as st
from ..objects import GenericObject
from ..enums import *
from .StrategyBase import StrategyBase
import copy
import itertools
from ..utils import time_scale_to_minute
from itertools import chain, groupby
import json

class AlwaysEnter90(StrategyBase):

    def __init__(self, _config, _symbol_info={}):
        super().__init__("AlwaysEnter90", _config, _symbol_info)
        return


    async def run(self, analysis_dict, lto_list, ikarus_time, total_qc, free_qc):
        return await super().run_logic(self, analysis_dict, lto_list, ikarus_time, total_qc, free_qc)


    async def make_decision(self, analysis_dict, ao_pair, ikarus_time, pairwise_alloc_share):

            # Make decision to enter or not
            if True:
                trade_obj = copy.deepcopy(GenericObject.trade)
                trade_obj['status'] = STAT_OPEN_ENTER
                trade_obj['strategy'] = self.name
                trade_obj['pair'] = ao_pair
                trade_obj['history'].append(trade_obj['status'])
                trade_obj['decision_time'] = int(ikarus_time) # Set decision_time to timestamp which is the open time of the current kline (newly started not closed kline)
                # TODO: give proper values to limit

                # Calculate enter/exit prices
                enter_price = float(analysis_dict[ao_pair][self.min_period]['low'][-1])*0.9 # NOTE: Give half of the price to make sure it will enter
                exit_price = float(analysis_dict[ao_pair][self.min_period]['high'][-1])*1.1 # NOTE: Give double of the price to make sure it will not exit
 
                # Calculate enter/exit amount value
                # Example: Buy XRP with 100$ in your account
                enter_ref_amount=pairwise_alloc_share

                # Fill enter module
                enter_type = self.config['enter']['type']
                trade_obj['enter'] = await StrategyBase._create_enter_module(
                    enter_type, 
                    enter_price, 
                    enter_ref_amount, 
                    StrategyBase._eval_future_candle_time(ikarus_time,0,time_scale_to_minute(self.min_period)))

                # Fill exit module
                exit_type = self.config['exit']['type']
                trade_obj['exit'] = await StrategyBase._create_exit_module(
                    exit_type,
                    enter_price,
                    trade_obj['enter'][enter_type]['quantity'],
                    exit_price,
                    StrategyBase._eval_future_candle_time(ikarus_time,9,time_scale_to_minute(self.min_period)))

                return trade_obj

            else:
                return None


    async def on_update(self, lto, ikarus_time):
        # TODO: Give a call to methods that calculates exit point
        # NOTE: Things to change: price, limitPrice, stopLimitPrice, expire date
        lto['action'] = ACTN_UPDATE 
        lto['update_history'].append(copy.deepcopy(lto['exit'][self.config['exit']['type']])) # This is good for debugging and will be good for perf. evaluation in future
        if self.config['exit']['type'] == TYPE_LIMIT:
            lto['exit'][TYPE_LIMIT]['price'] *= 1
            lto['exit'][TYPE_LIMIT]['amount'] = lto['exit'][TYPE_LIMIT]['price'] * lto['exit'][TYPE_LIMIT]['quantity']
            lto['exit'][TYPE_LIMIT]['expire'] = StrategyBase._eval_future_candle_time(ikarus_time,1,time_scale_to_minute(self.min_period))

        elif self.config['exit']['type'] == TYPE_OCO:
            lto['exit'][TYPE_OCO]['limitPrice'] *= 1
            lto['exit'][TYPE_OCO]['stopPrice'] *= 1
            lto['exit'][TYPE_OCO]['stopLimitPrice'] *= 1
            lto['exit'][TYPE_OCO]['amount'] = lto['exit'][TYPE_OCO]['limitPrice'] * lto['exit'][TYPE_OCO]['quantity']
            lto['exit'][TYPE_OCO]['expire'] = StrategyBase._eval_future_candle_time(ikarus_time,1,time_scale_to_minute(self.min_period))

        # Apply the filters
        # TODO: Add min notional fix (No need to add the check because we are not gonna do anything with that)
        lto['exit'][self.config['exit']['type']] = await StrategyBase.apply_exchange_filters(lto, self.symbol_info[lto['pair']])
        return lto


    async def on_exit_postpone(self, lto, ikarus_time):
        postponed_candles = 1
        lto = await StrategyBase._postpone(lto,'exit', self.config['exit']['type'], StrategyBase._eval_future_candle_time(ikarus_time,postponed_candles,time_scale_to_minute(self.min_period)))
        return lto


    async def on_enter_postpone(self, lto, ikarus_time):
        postponed_candles = 1
        lto = await StrategyBase._postpone(lto,'enter', self.config['enter']['type'], StrategyBase._eval_future_candle_time(ikarus_time,postponed_candles,time_scale_to_minute(self.min_period))) 
        return lto


    async def on_cancel(self, lto):
        lto['action'] = ACTN_CANCEL
        lto['result']['cause'] = STAT_ENTER_EXP
        return lto


    async def on_waiting_exit(self, lto, analysis_dict):
        lto['action'] = ACTN_EXEC_EXIT
        lto['exit'][self.config['exit']['type']] = await StrategyBase.apply_exchange_filters(lto, self.symbol_info[lto['pair']])
        return lto


    async def on_closed(self, lto):
        return lto
