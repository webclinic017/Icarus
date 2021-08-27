import logging
import statistics as st
from ..objects import GenericObject
from ..enums import *
from .StrategyBase import StrategyBase
import copy


class Str210827(StrategyBase):

    def __init__(self, _config, _symbol_info={}):
        self.name = "Str210827"
        self.logger = logging.getLogger('{}.{}'.format(__name__,self.name))

        self.config = _config['strategy'][self.name]

        self.quote_currency = _config['broker']['quote_currency']
        self.scales_in_minute = _config['data_input']['scales_in_minute']
        # TODO: Make proper handling for symbol_info
        self.symbol_info = _symbol_info

        return


    async def _handle_lto(self, lto, dt_index):
        """
        This function decides what to do for the LTOs based on their 'status'
        """        
        skip_calculation = False
        
        if lto['status'] == STAT_ENTER_EXP:
            if self.config['action_mapping'][STAT_ENTER_EXP] == ACTN_CANCEL or lto['history'].count(STAT_ENTER_EXP) > 1:
                lto['action'] = ACTN_CANCEL
                lto['result']['cause'] = STAT_ENTER_EXP

            elif self.config['action_mapping'][STAT_ENTER_EXP] == ACTN_POSTPONE and lto['history'].count(STAT_ENTER_EXP) <= 1:
                # NOTE: postponed_candles = 1 means 2 candle
                #       If only 1 candle is desired to be postponed, then it means we will wait for newly started candle to close so postponed_candles will be 0
                postponed_candles = 1
                lto = await StrategyBase._postpone(lto,'enter', self.config['enter']['type'], StrategyBase._eval_future_candle_time(dt_index,postponed_candles,self.scales_in_minute[0])) 
                skip_calculation = True
            else: pass

        elif lto['status'] == STAT_EXIT_EXP:

            if self.config['action_mapping'][STAT_EXIT_EXP] == ACTN_UPDATE:
                # TODO: Give a call to methods that calculates exit point
                # NOTE: Things to change: price, limitPrice, stopLimitPrice, expire date
                lto['action'] = ACTN_UPDATE 
                lto['update_history'].append(copy.deepcopy(lto['exit'][self.config['exit']['type']])) # This is good for debugging and will be good for perf. evaluation in future
                if self.config['exit']['type'] == TYPE_LIMIT:
                    lto['exit'][TYPE_LIMIT]['price'] *= 1
                    lto['exit'][TYPE_LIMIT]['amount'] = lto['exit'][TYPE_LIMIT]['price'] * lto['exit'][TYPE_LIMIT]['quantity']
                    lto['exit'][TYPE_LIMIT]['expire'] = StrategyBase._eval_future_candle_time(dt_index,3,self.scales_in_minute[0])

                elif self.config['exit']['type'] == TYPE_OCO:
                    lto['exit'][TYPE_OCO]['limitPrice'] *= 1
                    lto['exit'][TYPE_OCO]['amount'] = lto['exit'][TYPE_OCO]['limitPrice'] * lto['exit'][TYPE_OCO]['quantity']
                    lto['exit'][TYPE_OCO]['expire'] = StrategyBase._eval_future_candle_time(dt_index,3,self.scales_in_minute[0])
                skip_calculation = True

                # Apply the filters
                # TODO: Add min notional fix (No need to add the check because we are not gonna do anything with that)
                lto['exit'][self.config['exit']['type']] = await StrategyBase.apply_exchange_filters('exit', 
                                                                                                    self.config['exit']['type'], 
                                                                                                    lto['exit'][self.config['exit']['type']], 
                                                                                                    self.symbol_info, 
                                                                                                    exit_qty=lto['result']['enter']['quantity'])

            elif self.config['action_mapping'][STAT_EXIT_EXP] == ACTN_POSTPONE and lto['history'].count(STAT_EXIT_EXP) <= 1:
                postponed_candles = 1
                lto = await StrategyBase._postpone(lto,'exit', self.config['exit']['type'], StrategyBase._eval_future_candle_time(dt_index,postponed_candles,self.scales_in_minute[0]))
                skip_calculation = True

            elif self.config['action_mapping'][STAT_EXIT_EXP] == ACTN_MARKET_EXIT or lto['history'].count(STAT_EXIT_EXP) > 1:
                lto = await StrategyBase._config_market_exit(lto, self.config['exit']['type'])
                self.logger.info(f'LTO: market exit configured') # TODO: Add orderId

            else: pass

        elif lto['status'] == STAT_WAITING_EXIT:
            # LTO is entered succesfully, so exit order should be executed
            # TODO: expire of the exit_module can be calculated after the trade entered

            lto['action'] = ACTN_EXEC_EXIT
            lto['exit'][self.config['exit']['type']] = await StrategyBase.apply_exchange_filters('exit', 
                                                                                                self.config['exit']['type'], 
                                                                                                lto['exit'][self.config['exit']['type']], 
                                                                                                self.symbol_info, 
                                                                                                exit_qty=lto['result']['enter']['quantity'])
            skip_calculation = True

        elif lto['status'] != STAT_CLOSED:
            # If the status is not closed, just skip the iteration. otherwise go on to make a decision
            # NOTE: This logic contains the status: STAT_OPEN_EXIT, STAT_OPEN_ENTER, STAT_PART_CLOSED_ENTER, STAT_PART_CLOSED_EXIT
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
            list: nto_list
        """
        # Preliminary condition: all of the config['pairs'] exist in analysis_dict
        if not set(self.config['pairs']).issubset(analysis_dict.keys()):
            self.logger.warn(f"Configured pair \"{self.config['pairs']}\" does not exist in analysis_dict. Skipping {self.name}.run")
            return []

        # Initialize trade_dict to be filled
        trade_objects = []

        # Create a mapping between the pair and orderId such as {'BTCUSDT':['123','456']}
        pair_key_mapping = {}
        for i, lto in enumerate(lto_list):
            pair = lto['pair']
            if pair not in pair_key_mapping.keys():
                pair_key_mapping[pair] = []

            pair_key_mapping[pair].append(i)

        # This implementation enable to check number of trades and compare the value with the one in the config file.

        # NOTE: Only iterate for the configured pairs. Do not run the strategy if any of them is missing in analysis_dict
        for ao_pair in self.config['pairs']:

            # Check if there is already an LTO that has that 'pair' item. If so skip the evaluation (one pair one LTO rule)
            if ao_pair in pair_key_mapping.keys():
                
                # NOTE: If a pair contains multiple LTO then there should be another level of iteration as well
                skip_calculation, lto_list[pair_key_mapping[ao_pair][0]] = await self._handle_lto(lto_list[pair_key_mapping[ao_pair][0]], dt_index)
                if skip_calculation: continue;

            else: pass # Make a brand new decision
            
            time_dict = analysis_dict[ao_pair]
            # Since all parameters are handled in a different way, 
            # there needs to be different handlers for each type of indicator
            time_scales = list(time_dict.keys())
            session_time_scale = time_scales[0]
            trange_mean5 = st.mean(time_dict[session_time_scale]['trange'][-5:])
            trange_mean20 = st.mean(time_dict[session_time_scale]['trange'][-20:])

            # Make decision to enter or not
            if trange_mean5 < trange_mean20:
                self.logger.info(f"{ao_pair}: BUY SIGNAL")
                trade_obj = copy.deepcopy(GenericObject.trade)
                trade_obj['status'] = STAT_OPEN_ENTER
                trade_obj['strategy'] = self.name
                trade_obj['pair'] = ao_pair
                trade_obj['history'].append(trade_obj['status'])
                trade_obj['decision_time'] = int(dt_index) # Set decision_time to timestamp which is the open time of the current kline (newly started not closed kline)
                # TODO: give proper values to limit

                # Calculate enter/exit prices
                enter_price = min(time_dict[session_time_scale]['low'][-10:])
                exit_price = max(time_dict[session_time_scale]['high'][-10:])

                # Calculate enter/exit amount value

                #TODO: Amount calculation is performed to decide how much of the 'free' amount of 
                # the base asset will be used.

                free_ref_asset = df_balance.loc[self.quote_currency,'free']

                # Example: Buy XRP with 100$ in your account
                enter_ref_amount=20
                # TODO: HIGH: Check mininum amount to trade and add this section to here
                if free_ref_asset > 10:
                    if free_ref_asset < enter_ref_amount:
                        enter_ref_amount = free_ref_asset
                else:
                    # TODO: Add error logs and send notification
                    return {}

                # TODO: HIGH: In order to not to face with an issue with dust, exit amount might be "just a bit less" then what it should be
                # Example:
                #   Buy XRP from the price XRPUSDT: 0.66 (Price of 1XRP = 0.66$), use 100$ to make the trade
                #   151,51 = 100$ / 0.66
                enter_quantity = enter_ref_amount / enter_price

                #   Sell the bought XRP from the price 0.70
                #   exit_ref_amount = 151,4 * 0.70 = 105.98
                exit_ref_amount = enter_quantity * exit_price

                # Fill enter and exit modules
                enter_type = self.config['enter']['type']
                exit_type = self.config['exit']['type']

                trade_obj['enter'] = await StrategyBase._create_enter_module(enter_type, enter_price, enter_quantity, enter_ref_amount, 
                                                                        StrategyBase._eval_future_candle_time(dt_index,4,self.scales_in_minute[0])) # NOTE: Multiple scale is not supported
                trade_obj['exit'] = await StrategyBase._create_exit_module(exit_type, enter_price, enter_quantity, exit_price, exit_ref_amount, 
                                                                        StrategyBase._eval_future_candle_time(dt_index,9,self.scales_in_minute[0])) # NOTE: Multiple scale is not supported

                # TODO: Check the free amount of quote currency
                free_ref_asset = df_balance.loc[self.quote_currency,'free']

                trade_obj['enter'][self.config['enter']['type']] = await StrategyBase.apply_exchange_filters('enter', 
                                                                                                            enter_type, 
                                                                                                            trade_obj['enter'][enter_type], 
                                                                                                            self.symbol_info)

                if not await StrategyBase.check_min_notional(trade_obj['enter'][enter_type]['price'], trade_obj['enter'][enter_type]['quantity'], self.symbol_info):
                    # TODO: Notification about min_notional
                    continue
                trade_objects.append(trade_obj)

            else:
                self.logger.info(f"{ao_pair}: NO SIGNAL")

        return trade_objects

