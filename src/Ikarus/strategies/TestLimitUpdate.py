import statistics as st
from ..objects import ECause, Result, Trade, Limit, ECommand, TradeResult
from ..enums import *
from .StrategyBase import StrategyBase
import json
from ..utils import time_scale_to_minute

class TestLimitUpdate(StrategyBase):

    def __init__(self, _config, _symbol_info={}):
        super().__init__("TestLimitUpdate", _config, _symbol_info)
        return


    async def run(self, analysis_dict, lto_list, ikarus_time, total_qc, free_qc):
        return await super().run_logic(self, analysis_dict, lto_list, ikarus_time, total_qc, free_qc)


    async def make_decision(self, analysis_dict, ao_pair, ikarus_time, pairwise_alloc_share):

            time_dict = analysis_dict[ao_pair]

            # Calculate enter/exit prices
            enter_price = time_dict[self.min_period]['close'] * 1.05 # Enter
            exit_price = time_dict[self.min_period]['close'] * 1.6
            enter_ref_amount=pairwise_alloc_share

            enter_limit_order = Limit(
                enter_price,
                amount=enter_ref_amount,
                expire=StrategyBase._eval_future_candle_time(ikarus_time,15,time_scale_to_minute(self.min_period))
            )

            exit_limit_order = Limit(
                exit_price,
                quantity=enter_limit_order.quantity,
                expire=StrategyBase._eval_future_candle_time(ikarus_time,15,time_scale_to_minute(self.min_period))
            )

            # Set decision_time to timestamp which is the open time of the current kline (newly started not closed kline)
            trade = Trade(int(ikarus_time), self.name, ao_pair, command=ECommand.EXEC_ENTER)
            trade.set_enter(enter_limit_order)
            trade.set_exit(exit_limit_order)
            result = TradeResult()
            result.enter, result.exit = Result(), Result()
            trade.result = result

            return trade


    async def on_update(self, trade, ikarus_time, **kwargs):
        # NOTE: Things to change: price, limitPrice, stopLimitPrice, expire date
        close_price = kwargs['analysis_dict'][trade.pair][self.min_period]['close'] 
        trade.command = ECommand.UPDATE
        trade.stash_exit()
        trade.exit.orderId = None
        trade.exit.expire = StrategyBase._eval_future_candle_time(ikarus_time,3,time_scale_to_minute(self.min_period))
        trade.exit.set_price(close_price*0.95)

        if not StrategyBase.apply_exchange_filters(trade.exit, self.symbol_info[trade.pair]):
            return False
        return True


    async def on_cancel(self, trade):
        trade.command = ECommand.CANCEL
        trade.result.cause = ECause.ENTER_EXP
        return True


    async def on_waiting_exit(self, trade, analysis_dict):
        trade.command = ECommand.EXEC_EXIT
        if not StrategyBase.apply_exchange_filters(trade.exit, self.symbol_info[trade.pair]):
            return False
        return True


    async def on_closed(self, lto):
        return lto
