import logging
from binance.helpers import round_step_size
from .enums import *
from . import strategies

class StrategyManager():

    def __init__(self, _config , _symbol_info) -> None:

        self.strategies = {
            "OCOBackTest": strategies.OCOBackTest.OCOBackTest,
            "AlwaysEnter": strategies.AlwaysEnter.AlwaysEnter,
            "AlwaysEnter90": strategies.AlwaysEnter90.AlwaysEnter90,
            "FallingKnifeCatcher": strategies.FallingKnifeCatcher.FallingKnifeCatcher,
            "Str210827": strategies.Str210827.Str210827,
        }
        # TODO: Find a way to get rid of manual strategy adding process
        self.strategy_list = []
        for strategy_name in _config['strategy'].keys():
            strategy_class = self.strategies[strategy_name]
            self.strategy_list.append(strategy_class(_config, _symbol_info))
        pass

    def get_strategies(self): return self.strategy_list

    def remove_strategy(self): return True

    def add_strategy(self): return True