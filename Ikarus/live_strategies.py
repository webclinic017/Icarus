import logging
import statistics as st
import json
from Ikarus.objects import GenericObject, ObjectEncoder
from binance.helpers import round_step_size
import bson
import copy
import abc
import time

class StrategyBase(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def __init__(self):
        pass

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'run') and 
                callable(subclass.run) and 
                hasattr(subclass, 'dump_to') and 
                callable(subclass.dump_to) or 
                NotImplemented)

    @abc.abstractmethod
    async def run(self, analysis_dict, lto_dict, df_balance, dt_index=None):
        """Load in the data set"""
        raise NotImplementedError

    abc.abstractmethod
    async def dump_to(self, js_obj: dict):
        js_file = open("run-time-objs/trade.json", "w")
        json.dump(js_obj, js_file, indent=4, cls=ObjectEncoder)
        js_file.close()
        self.logger.debug("trade.json file created")

        return True


class AlwaysEnter(StrategyBase):

    def __init__(self, _config):
        self.logger = logging.getLogger('app.{}'.format(__name__))
        self.config = _config['strategy']
        self.quote_currency = _config['broker']['quote_currency']
        self.scales_in_minute = _config['data_input']['scales_in_minute']

        # TODO: Make proper handling for symbol_info
        self.symbol_info = dict()
        return

    async def _postpone(self, lto, phase, expire_time):
        lto['action'] = 'postpone'
        lto[phase][self.config[phase]['type']]['expire'] = expire_time
        return lto


    def _eval_future_candle_time(self, start_time, count, minute): return bson.Int64(start_time + count*minute*60*1000)


    async def _config_market_exit(self, lto):

        lto['action'] = 'market_exit'
        lto['exit']['market'] = {
            'amount': lto['exit'][self.config['exit']['type']]['amount'],
            'quantity': lto['exit'][self.config['exit']['type']]['quantity'],
            'orderId': '',
        }
        return lto


    async def _create_enter_module(self, enter_price, enter_quantity, enter_ref_amount, expire_time):

        if self.config['enter']['type'] == 'limit':
            enter_module = {
                "limit": {
                    "price": float(enter_price),
                    "quantity": float(enter_quantity),
                    "amount": float(enter_ref_amount),
                    "expire": expire_time,
                    "orderId": ""
                    },
                }
        elif self.config['enter']['type'] == 'market':
            # TODO: Create 'market' orders to enter
            pass
        else: pass # Internal Error
        return enter_module


    async def _create_exit_module(self, enter_price, enter_quantity, exit_price, exit_ref_amount, expire_time):

        if self.config['exit']['type'] == 'oco':
            exit_module = {
                "oco": {
                    "limitPrice": float(exit_price),
                    "stopPrice": float(enter_price)*0.995,           # Auto-execute stop loss if the amount go below %0.05
                    "stopLimitPrice": float(enter_price)*0.994,      # Lose max %0.06 of the amount
                    "quantity": float(enter_quantity),
                    "amount": float(exit_ref_amount),
                    "expire": expire_time,
                    "orderId": "",
                    "stopLimit_orderId": ""
                }
            }
        elif self.config['exit']['type'] == 'limit':
            exit_module = {
                "limit": {
                    "price": float(exit_price),
                    "quantity": float(enter_quantity),
                    "amount": float(exit_ref_amount),
                    "expire": expire_time,
                    "orderId": ""
                    },
                }
        elif self.config['exit']['type'] == 'market':
            pass
        else: pass # Internal Error
        return exit_module


    async def apply_exchange_filters(self, to, phase):
        """
        - Call this method prior to any order placement

        - Apply the filter of exchange pair

        - This methhod does not check if the current conditiones are good to go.
            If a filter is not satisfied then it would create an exception. Validation
            costs time. Maybe in future 

        - When it is time to place the exit order, exit price might be updated, so this method shpudl be called
        Returns:
            dict: [description]
        """ 
        '''
        if free_ref_asset > self.symbol_info[]:
            if free_ref_asset < enter_ref_amount:
                enter_ref_amount = free_ref_asset
        else:
            # TODO: ERROR: NO free asset
            return {}
        '''

        if phase == 'enter':
            # Fixing PRICE_FILTER: tickSize
            to[phase][self.config[phase]['type']]['price'] = round_step_size(to[phase][self.config[phase]['type']]['price'], 
                                                                                    float(self.symbol_info['filters'][0]['tickSize'])) 
            # Fixing LOT_SIZE: minQty
            to[phase][self.config[phase]['type']]['quantity'] = round_step_size(to[phase][self.config[phase]['type']]['amount'] / to[phase][self.config[phase]['type']]['price'], 
                                                                                    float(self.symbol_info['filters'][2]['minQty']))

        elif phase == 'exit':
            if self.config[phase]['type'] == 'oco':
                # Fixing PRICE_FILTER: tickSize
                # TODO: NEXT: Optimize the this mess
                to[phase][self.config[phase]['type']]['limitPrice'] = round_step_size(to[phase][self.config[phase]['type']]['limitPrice'], 
                                                                                        float(self.symbol_info['filters'][0]['tickSize']))

                to[phase][self.config[phase]['type']]['stopPrice'] = round_step_size(to[phase][self.config[phase]['type']]['stopPrice'], 
                                                                                        float(self.symbol_info['filters'][0]['tickSize']))

                to[phase][self.config[phase]['type']]['stopLimitPrice'] = round_step_size(to[phase][self.config[phase]['type']]['stopLimitPrice'], 
                                                                                        float(self.symbol_info['filters'][0]['tickSize']))

                # Fixing LOT_SIZE: minQty
                to[phase][self.config[phase]['type']]['quantity'] = round_step_size(to[phase][self.config[phase]['type']]['amount'] / to[phase][self.config[phase]['type']]['limitPrice'], 
                                                                                        float(self.symbol_info['filters'][2]['minQty']))
                # TODO: NEXT: How to define quantity: use the quantity in the enter phase
            
            elif self.config[phase]['type'] == 'limit':
                to[phase][self.config[phase]['type']]['price'] = round_step_size(to[phase][self.config[phase]['type']]['price'], 
                                                                                        float(self.symbol_info['filters'][0]['tickSize']))
                # Fixing LOT_SIZE: minQty
                to[phase][self.config[phase]['type']]['quantity'] = round_step_size(to[phase][self.config[phase]['type']]['amount'] / to[phase][self.config[phase]['type']]['price'], 
                                                                                        float(self.symbol_info['filters'][2]['minQty']))

        # NOTE: Doing if else is for exit types are annoying

        return to


    async def _handle_lto(self, lto, dt_index):
        skip_calculation = False
        
        if lto['status'] == 'enter_expire':
            if self.config['action_mapping']['enter_expire'] == 'cancel' or lto['history'].count('enter_expire') > 1:
                lto['action'] = 'cancel'
                lto['result']['cause'] = 'enter_expire'

            elif self.config['action_mapping']['enter_expire'] == 'postpone' and lto['history'].count('enter_expire') <= 1:
                # NOTE: postponed_candles = 1 means 2 candle
                #       If only 1 candle is desired to be postponed, then it means we will wait for newly started candle to close so postponed_candles will be 0
                postponed_candles = 1
                lto = await self._postpone(lto,'enter', self._eval_future_candle_time(dt_index,postponed_candles,self.scales_in_minute[0])) 
                skip_calculation = True
            else: pass

        elif lto['status'] == 'exit_expire':
            if self.config['action_mapping']['exit_expire'] == 'market_exit' or lto['history'].count('exit_expire') > 1:
                lto = await self._config_market_exit(lto)
                self.logger.info(f'LTO: market exit configured') # TODO: Add orderId

            elif self.config['action_mapping']['exit_expire'] == 'postpone' and lto['history'].count('exit_expire') <= 1:
                postponed_candles = 1
                lto = await self._postpone(lto,'exit', self._eval_future_candle_time(dt_index,postponed_candles,self.scales_in_minute[0]))
                skip_calculation = True
            else: pass


        elif lto['status'] == 'waiting_exit':
            # LTO is entered succesfully, so exit order should be executed
            # TODO: expire of the exit_module can be calculated after the trade entered

            lto['action'] = 'execute_exit'
            lto = await self.apply_exchange_filters(lto, phase='exit')
            skip_calculation = True

        elif lto['status'] != 'closed':
            # If the status is not closed, just skip the iteration. otherwise go on to make a decision
            # NOTE: This logic contains the status: 'open_exit', 'open_enter', 'partially_closed_enter', 'partially_closed_exit'
            skip_calculation = True

        return skip_calculation, lto


    async def run(self, analysis_dict, lto_list, df_balance, dt_index=None):
        """
        It requires to feed analysis_dict and lto_dict so that it may decide to:
        - not to enter a new trade if there is already an open trade
        - cancel the trade if an drawdown is detected

        Args:
            analysis_dict (dict): analysis.json
            - analysis objects contains where to buy and and where to sell

            lto_dict (dict): live-trade-objects coming from the [live-trades]

            df_balance (pd.DataFrame): live-trade-objects coming from the [live-trades]

            dt_index (int): timestamp in ms for trade_object identifier
            
        Returns:
            dict: trade.json
        """
        # Initialize trade_dict to be filled
        trade_objects = []

        # Create a mapping between the pair and index such as {'BTCUSDT':[0,3]}
        pair_index_mapping = {}
        for i, lto in enumerate(lto_list):
            pair = lto['pair']
            if pair not in pair_index_mapping.keys():
                pair_index_mapping[pair] = []

            pair_index_mapping[pair].append(i)

        # This implementation enable to check number of trades and compare the value with the one in the config file.

        # TODO: Consider the fact that an pair have multiple to's going on. Max number can be restricted
        for ao_pair in analysis_dict.keys():

            # Check if there is already an LTO that has that 'pair' item. If so skip the evaluation (one pair one LTO rule)
            if ao_pair in pair_index_mapping.keys():
                
                # NOTE: If a pair contains multiple LTO then there should be another level of iteration as well
                skip_calculation, lto_list[pair_index_mapping[ao_pair][0]] = await self._handle_lto(lto_list[pair_index_mapping[ao_pair][0]], dt_index)
                if skip_calculation: continue;

            else: pass # Make a brand new decision
            
            assert len(analysis_dict[ao_pair].keys()) == 1, "Multiple time scale is not supported"
            scale = list(analysis_dict[ao_pair].keys())[0]

            # Make decision to enter or not
            if True:
                self.logger.info(f"{ao_pair}: BUY SIGNAL")
                trade_obj = copy.deepcopy(GenericObject.trade)
                trade_obj['status'] = 'open_enter'
                trade_obj['pair'] = ao_pair
                trade_obj['history'].append(trade_obj['status'])
                trade_obj['decision_time'] = int(dt_index) # Set decision_time to the the open time of the current kline not the last closed kline

                # Calculate enter/exit prices
                enter_price = float(analysis_dict[ao_pair][scale]['low'][-1])/2 # NOTE: Give half of the price to make sure it will enter
                exit_price = float(analysis_dict[ao_pair][scale]['high'][-1])*2 # NOTE: Give double of the price to make sure it will not exit
                
                # Example: Buy XRP with 100$ in your account
                enter_ref_amount=100

                #   Buy XRP from the price XRPUSDT: 0.66 (Price of 1XRP = 0.66$), use 100$ to make the trade
                #   151,51 = 100$ / 0.66
                enter_quantity = enter_ref_amount / enter_price

                #   Sell the bought XRP from the price 0.70
                #   exit_ref_amount = 151,4 * 0.70 = 105.98
                exit_ref_amount = enter_quantity * exit_price

                # Fill enter and exit modules
                # TODO: Expire calculation should be based on the 'scale'. It should not be hardcoded '15'
                trade_obj['enter'] = await self._create_enter_module(enter_price, enter_quantity, enter_ref_amount, 
                                                                        self._eval_future_candle_time(dt_index,0,self.scales_in_minute[0])) # NOTE: Multiple scale is not supported
                trade_obj['exit'] = await self._create_exit_module(enter_price, enter_quantity, exit_price, exit_ref_amount, 
                                                                        self._eval_future_candle_time(dt_index,0,self.scales_in_minute[0])) # NOTE: Multiple scale is not supported
                trade_obj['_id'] = int(time.time() * 1000)

                # TODO: Check the free amount of quote currency
                free_ref_asset = df_balance.loc[self.quote_currency,'free']

                # TODO: Apply filter function needs to be added to met the pair-exchange requirements
                trade_obj = await self.apply_exchange_filters(trade_obj, phase='enter')

                # Normally trade id should be unique and be given by the broker. Until it is executed assign the current ts. It will be updated at execution anyway
                trade_objects.append(trade_obj)

            else:
                self.logger.info(f"{ao_pair}: NO SIGNAL")

        return trade_objects

    async def dump_to(self, js_obj: dict):
        js_file = open("run-time-objs/trade.json", "w")
        json.dump(js_obj, js_file, indent=4, cls=ObjectEncoder)
        js_file.close()
        self.logger.debug("trade.json file created")

        return True
