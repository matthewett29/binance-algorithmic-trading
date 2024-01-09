import json
from pandas import DataFrame
from abc import ABC, abstractmethod
from binance_algorithmic_trading.logger import Logger


class BaseStrategy(ABC):
    '''
    A base strategy class for use as a template for all strategies.

    The base class contains all functions and attributes for backtesting, live
    trading and calculating performance statistics. These are implemented here
    such that they can be inherited for new strategies rather than duplicating
    functionality.
    '''

    def __init__(self, log_level, database_manager):

        self._logger = Logger(logger_name=__file__,
                              log_level=log_level).get_logger()
        
        self._database_manager = database_manager

        self.name = None

        # Attributes initialised from params in the backtest function
        self.symbol = None
        self.backtest_start_time = None
        self.backtest_end_time = None
        self.entry_interval = None
        self.trade_interval = None
        self.params = None
        self.starting_capital = None
        self.risk_percentage = None
        self.current_capital = None
        self.risk_percentage = None
        self.use_BNB_for_commission = None

        # Metrics used for backtesting stats
        self.total_gross_profit = 0
        self.total_fees = 0
        self.total_net_profit = 0
        self.profit_factor = 0
        self.max_drawdown = 0
        self.total_trades = 0
        self.total_wins = 0
        self.total_losses = 0
        self.total_breakevens = 0
        self.max_win_streak = 0
        self.max_loss_streak = 0
        self.total_SL_exits_ALL = 0
        self.total_TSL_exits_ALL = 0
        self.total_EOTB_exits_ALL = 0
        self.avg_win_profit_perc_ALL = 0
        self.avg_loss_profit_perc_ALL = 0
        self.max_win_profit_perc_ALL = 0
        self.max_loss_profit_perc_ALL = 0
        self.min_win_profit_perc_ALL = 0
        self.min_loss_profit_perc_ALL = 0

        # Initialise backtest statistics dataframe
        self._backtest_stats = DataFrame(
            columns=[
                'strategy_name',
                'symbol',
                'start_time',
                'end_time',
                'entry_interval',
                'trade_interval',
                'params',
                'risk_percentage',
                'starting_capital',
                'remaining_capital',
                'gross_profit',
                'fees',
                'net_profit',
                'profit_factor',
                'max_drawdown',
                'total_trades',
                'total_wins',
                'total_losses',
                'max_win_streak',
                'max_loss_streak',
                'SL_exits',
                'TSL_exits',
                'EOTB_exits',
                'avg_win_return',
                'avg_loss_return',
                'max_win_return',
                'max_loss_return',
                'min_win_return',
                'min_loss_return'
            ]
        )

    def backtest(self, starting_capital, risk_percentage, symbols, start_time,
                 end_time, entry_interval, trade_interval, params,
                 use_BNB_for_commission):
        '''
        Backtest trades using the strategy execution logic.

        For LONG trades:

        Start Trading Trigger Rules
        ---------------------------
        A1. 15m candle price moves and closes higher than the highest close
            price of the previous N 15m candles
        A2. Volume is more than Y multiples of the average volume of the
            previous N 15m candles

        Consecutive Trade Trigger Rules
        -------------------------------
        B1. Start Trading Trigger (A) has occurred
        B2. Stop Trading Trigger (D) has not yet occurred
        B3. Not currently in a trade
        B4. Use 1m candles to find entries
        B4. Price equals the current 15m candle open price

        Trade Execution Rules
        ---------------------
        C1. BUY when (A) or (B) Trading Triggers occur
        C2. Stop Loss set X pips below entry price
        C3. No Take Profit level, trade exits when SL hit
        C4. Move SL X pips below price of each new 15m candle open price

        Stop Trading Trigger Rules
        --------------------------
        D1. A 15m candle closes below the open of the last bullish 15m candle

        Tuning
        ------
        N:  number of candles to use for determining the highest close price
            and average volume values which affect the Start Trading Trigger
        X:  number of pips to use for a SL (maybe base on ATR?)

        Limitations
        -----------
        In backtesting, the Consecutive Trade Trigger Rules specify that a
        trade should be entered everytime price equals the entry price of the
        current 15m candle if not already in a trade. Due to only using a 1m
        candle to find these entries, there may be multiple entry triggers
        within the 1m candle that aren't accounted for. If there were multiple
        triggers and within the 1m candle price moved down X pips to hit the
        SL level, then there would be unaccounted for losses. If this were the 
        case, then the strategy may be less performant than results show after
        backtesting. It is important to consider this and monitor the strategy
        performance during live testing. Especially consider this limitation
        when tuning the SL parameter X as a small SL may be triggered more
        often within the movement of a 1m candle.
        '''

        self._logger.info(f"starting backtest using {self.name} strategy "
                          f"between {start_time} and {end_time}")

        symbols = json.loads(symbols)
        self.backtest_start_time = start_time
        self.backtest_end_time = end_time
        self.entry_interval = entry_interval
        self.trade_interval = trade_interval
        self.params = params
        self.risk_percentage = risk_percentage
        self.starting_capital = starting_capital
        self.current_capital = starting_capital
        self.use_BNB_for_commission = use_BNB_for_commission

        # Initialise trade log dataframe
        self._trade_log = DataFrame(
            columns=[
                'symbol',
                'interval',
                'direction_long',
                'trade_block_id',
                'quantity',
                'entry_time',
                'entry_price',
                'stop_loss_price',
                'take_profit_price',
                'exit_trigger',
                'exit_time',
                'exit_price',
                'gross_profit',
                'net_profit',
                'fees',
                'remaining_capital',
                'drawdown'
            ]
        )

        # Backtest for all symbols
        for symbol in symbols:

            self.symbol = symbol
            self._execute_strategy()
            self._logger.info(f"finished backtest for {symbol}")

        self._logger.info("finished backtesting")

        # Calculate stats for the strategy
        self._calculate_stats()

    def get_stats(self):
        return self._backtest_stats

    def print_stats(self):
        '''
        Print all performance metrics
        '''
        print("---------------------------------------------------")
        print(f"****** {self.name} Strategy Performance ******")
        print("---------------------------------------------------")
        print("SETTINGS")
        print(f"\tStarting Balance $:\t\t      {self.starting_capital}")
        print(f"\tCapital Risked Per Trade %:\t\t  {self.risk_percentage}")
        print(f"\tParams: {self.params}")
        print(f"\tStart Time:\t\t{self.backtest_start_time.strftime('%Y-%m-%d %H:%M:00')}")
        print(f"\tEnd Time:\t\t{self.backtest_end_time.strftime('%Y-%m-%d %H:%M:00')}")
        print("---------------------------------------------------")
        print("TOTALS \t\t\tALL\tLONG\tSHORT")
        print(f"\tTrades:\t\t{self.total_trades_ALL}\t{self.total_trades_LONG}\t{self.total_trades_SHORT}")
        print(f"\tWins:\t\t{self.total_wins_ALL}\t{self.total_wins_LONG}\t{self.total_wins_SHORT}")
        print(f"\tLosses:\t\t{self.total_losses_ALL}\t{self.total_losses_LONG}\t{self.total_losses_SHORT}")
        print(f"\tBreakeven:\t{self.total_breakevens_ALL}\t{self.total_breakevens_LONG}\t{self.total_breakevens_SHORT}")
        print("")
        print(f"\tSL Exits:\t{self.total_SL_exits_ALL}\t{self.total_SL_exits_LONG}\t{self.total_SL_exits_SHORT}")
        print(f"\tTSL Exits:\t{self.total_TSL_exits_ALL}\t{self.total_TSL_exits_LONG}\t{self.total_TSL_exits_SHORT}")
        print(f"\tTSL Win Exits:\t{self.total_TSL_win_exits_ALL}\t{self.total_TSL_win_exits_LONG}\t{self.total_TSL_win_exits_SHORT}")
        print(f"\tTSL Loss Exits:\t{self.total_TSL_loss_exits_ALL}\t{self.total_TSL_loss_exits_LONG}\t{self.total_TSL_loss_exits_SHORT}")
        print(f"\tEOTB Exits:\t{self.total_EOTB_exits_ALL}\t{self.total_EOTB_exits_LONG}\t{self.total_EOTB_exits_SHORT}")
        print("RATES")
        print(f"\tWin Rate:\t{self.win_rate_ALL}\t{self.win_rate_LONG}\t{self.win_rate_SHORT}")
        print(f"\tLoss Rate:\t{self.loss_rate_ALL}\t{self.loss_rate_LONG}\t{self.loss_rate_SHORT}")
        print(f"\tBreakeven Rate:\t{self.breakeven_rate_ALL}\t{self.breakeven_rate_LONG}\t{self.breakeven_rate_SHORT}")
        print("WINS")
        print(f"\tMax Profit %:\t{self.max_win_profit_perc_ALL}\t{self.max_win_profit_perc_LONG}\t{self.max_win_profit_perc_SHORT}")
        print(f"\tMin Profit %:\t{self.min_win_profit_perc_ALL}\t{self.min_win_profit_perc_LONG}\t{self.min_win_profit_perc_SHORT}")
        print(f"\tAvg Profit %:\t{self.avg_win_profit_perc_ALL}\t{self.avg_win_profit_perc_LONG}\t{self.avg_win_profit_perc_SHORT}")
        # print("")
        # print(f"\tMax Profit $:\t{self.max_win_profit_ALL}\t{self.max_win_profit_LONG}\t{self.max_win_profit_SHORT}")
        # print(f"\tMin Profit $:\t{self.min_win_profit_ALL}\t{self.min_win_profit_LONG}\t{self.min_win_profit_SHORT}")
        # print(f"\tAvg Profit $:\t{self.avg_win_profit_ALL}\t{self.avg_win_profit_LONG}\t{self.avg_win_profit_SHORT}")
        print("LOSSES")
        print(f"\tMax Loss %:\t{self.max_loss_profit_perc_ALL}\t{self.max_loss_profit_perc_LONG}\t{self.max_loss_profit_perc_SHORT}")
        print(f"\tMin Loss %:\t{self.min_loss_profit_perc_ALL}\t{self.min_loss_profit_perc_LONG}\t{self.min_loss_profit_perc_SHORT}")
        print(f"\tAvg Loss %:\t{self.avg_loss_profit_perc_ALL}\t{self.avg_loss_profit_perc_LONG}\t{self.avg_loss_profit_perc_SHORT}")
        # print("")
        # print(f"\tMax Loss $:\t{self.max_loss_profit_ALL}\t{self.max_loss_profit_LONG}\t{self.max_loss_profit_SHORT}")
        # print(f"\tMin Loss $:\t{self.min_loss_profit_ALL}\t{self.min_loss_profit_LONG}\t{self.min_loss_profit_SHORT}")
        # print(f"\tAvg Loss $:\t{self.avg_loss_profit_ALL}\t{self.avg_loss_profit_LONG}\t{self.avg_loss_profit_SHORT}")
        print("PERFORMANCE")
        print(f"\tProfit Factor:\t\t{self.profit_factor}")
        print(f"\tMax Drawdown %:\t\t{self.max_drawdown}")
        print(f"\tGross Profit $:\t\t{round(self.total_gross_profit, 2)}")
        print(f"\tNet Profit $:\t\t{round(self.total_net_profit, 2)}")
        print(f"\tCommission $:\t\t{round(self.total_fees, 2)}")
        print(f"\tEnding Balance $:\t{round(self.current_capital, 2)}")
        print("---------------------------------------------------")

        # EOTB_trades = self._trade_log.query('exit_trigger == "END_OF_TRADING_BLOCK"')
        # last_EOTB_trade_block_id = EOTB_trades['trade_block_id'].max()
        # last_EOTB_trade_block_trades = EOTB_trades.query('trade_block_id == @last_EOTB_trade_block_id')
        # print(last_EOTB_trade_block_trades)

    def get_trade_log(self):
        return self._trade_log

    @abstractmethod
    def _execute_strategy(self):
        pass

    def _log_trade(self, symbol, interval, direction_long, trade_block_id,
                   quantity, entry_time, entry_price, stop_loss_price,
                   take_profit_price, exit_trigger, exit_time, exit_price,
                   gross_profit, net_profit, commission, remaining_capital,
                   drawdown):

        self._trade_log.loc[len(self._trade_log)] = ([
            symbol, interval, direction_long, trade_block_id, quantity,
            entry_time, entry_price, stop_loss_price, take_profit_price,
            exit_trigger, exit_time, exit_price, gross_profit, net_profit,
            commission, remaining_capital, drawdown]
        )

    def _calculate_profit(self, quantity, direction_long, entry_price,
                          exit_price, use_BNB_for_commission):
        
        # Calculate commission to be paid
        if use_BNB_for_commission:
            commission = quantity * (entry_price + exit_price) * 0.00075
        else:
            commission = quantity * (entry_price + exit_price) * 0.001

        # Calculate gross and net profit
        gross_profit = quantity * (exit_price - entry_price)
        net_profit = gross_profit - commission

        return gross_profit, net_profit, commission

    def _calculate_order_quantity(self, entry_price, stop_loss_price):

        # Calculate max quantity possible to buy using all account capital
        max_quantity = float(self.current_capital / entry_price)

        # Calculate quantity based on SL distance and risk per trade
        loss_price_difference = abs(entry_price - stop_loss_price)
        loss_value_allowed = (
            float(self.risk_percentage / 100) * self.current_capital
        )
        quantity = float(loss_value_allowed / loss_price_difference)

        # Limit the quantity to the max possible
        if quantity > max_quantity:
            quantity = max_quantity

        # TO DO: round quantity decimals to the allowed size based
        # on binance asset price resolution

        return quantity

    def _calculate_stats(self):

        self.win_rate_SHORT = None
        self.loss_rate_SHORT = None
        self.breakeven_rate_SHORT = None
        self.win_rate_LONG = None
        self.loss_rate_LONG = None
        self.breakeven_rate_LONG = None
        self.win_rate_ALL = None
        self.loss_rate_ALL = None
        self.breakeven_rate_ALL = None
        self.max_win_profit_ALL = None
        self.max_win_profit_perc_ALL = None
        self.max_win_profit_LONG = None
        self.max_win_profit_perc_LONG = None
        self.max_win_profit_SHORT = None
        self.max_win_profit_perc_SHORT = None
        self.min_win_profit_ALL = None
        self.min_win_profit_perc_ALL = None
        self.min_win_profit_LONG = None
        self.min_win_profit_perc_LONG = None
        self.min_win_profit_SHORT = None
        self.min_win_profit_perc_SHORT = None
        self.avg_win_profit_ALL = None
        self.avg_win_profit_perc_ALL = None
        self.avg_win_profit_LONG = None
        self.avg_win_profit_perc_LONG = None
        self.avg_win_profit_SHORT = None
        self.avg_win_profit_perc_SHORT = None
        self.max_loss_profit_ALL = None
        self.max_loss_profit_perc_ALL = None
        self.max_loss_profit_LONG = None
        self.max_loss_profit_perc_LONG = None
        self.max_loss_profit_SHORT = None
        self.max_loss_profit_perc_SHORT = None
        self.min_loss_profit_ALL = None
        self.min_loss_profit_perc_ALL = None
        self.min_loss_profit_LONG = None
        self.min_loss_profit_perc_LONG = None
        self.min_loss_profit_SHORT = None
        self.min_loss_profit_perc_SHORT = None
        self.avg_loss_profit_ALL = None
        self.avg_loss_profit_perc_ALL = None
        self.avg_loss_profit_LONG = None
        self.avg_loss_profit_perc_LONG = None
        self.avg_loss_profit_SHORT = None
        self.avg_loss_profit_perc_SHORT = None
        self.total_SL_exits_ALL = None
        self.total_SL_exits_LONG = None
        self.total_SL_exits_SHORT = None
        self.total_TSL_exits_ALL = None
        self.total_TSL_exits_LONG = None
        self.total_TSL_exits_SHORT = None
        self.total_TSL_win_exits_ALL = None
        self.total_TSL_win_exits_LONG = None
        self.total_TSL_win_exits_SHORT = None
        self.total_TSL_loss_exits_ALL = None
        self.total_TSL_loss_exits_LONG = None
        self.total_TSL_loss_exits_SHORT = None
        self.total_EOTB_exits_ALL = None
        self.total_EOTB_exits_LONG = None
        self.total_EOTB_exits_SHORT = None

        self.max_SL_hits_in_trading_block_ALL = None
        self.max_SL_hits_in_trading_block_LONG = None
        self.max_SL_hits_in_trading_block_SHORT = None
        self.min_SL_hits_in_trading_block_ALL = None
        self.min_SL_hits_in_trading_block_LONG = None
        self.min_SL_hits_in_trading_block_SHORT = None
        self.avg_SL_hits_in_trading_block_ALL = None
        self.avg_SL_hits_in_trading_block_LONG = None
        self.avg_SL_hits_in_trading_block_SHORT = None

        results_ALL = self._trade_log
        results_LONG = results_ALL.query('direction_long == 1')
        results_SHORT = results_ALL.query('direction_long == 0')

        # Get dataframes for wins, losses and breakevens
        wins_ALL = results_ALL.query('net_profit > 0')
        wins_LONG = wins_ALL.query('direction_long == 1')
        wins_SHORT = wins_ALL.query('direction_long == 0') 
        losses_ALL = results_ALL.query('net_profit < 0')
        losses_LONG = losses_ALL.query('direction_long == 1')
        losses_SHORT = losses_ALL.query('direction_long == 0') 
        breakevens_ALL = results_ALL.query('net_profit == 0')
        breakevens_LONG = breakevens_ALL.query('direction_long == 1')
        breakevens_SHORT = breakevens_ALL.query('direction_long == 0') 

        # Calculate total trades
        self.total_trades_ALL = len(results_ALL.index)
        self.total_trades_LONG = len(results_LONG.index)
        self.total_trades_SHORT = len(results_SHORT.index)

        # Calculate total wins and losses
        self.total_wins_ALL = len(wins_ALL.index)
        self.total_wins_LONG = len(wins_LONG.index)
        self.total_wins_SHORT = len(wins_SHORT.index)
        self.total_losses_ALL = len(losses_ALL.index)
        self.total_losses_LONG = len(losses_LONG.index)
        self.total_losses_SHORT = len(losses_SHORT.index)
        self.total_breakevens_ALL = len(breakevens_ALL.index)
        self.total_breakevens_LONG = len(breakevens_LONG.index)
        self.total_breakevens_SHORT = len(breakevens_SHORT.index)

        # Calculate win, loss and breakeven rates
        if self.total_trades_ALL > 0:
            self.win_rate_ALL = round(
                self.total_wins_ALL / self.total_trades_ALL, 3)
            self.loss_rate_ALL = round(
                self.total_losses_ALL / self.total_trades_ALL, 3)
            self.breakeven_rate_ALL = round(
                self.total_breakevens_ALL / self.total_trades_ALL, 3)
            
        if self.total_trades_LONG > 0:
            self.win_rate_LONG = round(
                self.total_wins_LONG / self.total_trades_LONG, 3)
            self.loss_rate_LONG = round(
                self.total_losses_LONG / self.total_trades_LONG, 3)
            self.breakeven_rate_LONG = round(
                self.total_breakevens_LONG / self.total_trades_LONG, 3)

        if self.total_trades_SHORT > 0:
            self.win_rate_SHORT = round(
                self.total_wins_SHORT / self.total_trades_SHORT, 3)
            self.loss_rate_SHORT = round(
                self.total_losses_SHORT / self.total_trades_SHORT, 3)
            self.breakeven_rate_SHORT = round(
                self.total_breakevens_SHORT / self.total_trades_SHORT, 3)

        # Calculate max, min and avg profit values and percentages for wins
        self.max_win_profit_ALL, self.max_win_profit_perc_ALL = (
            self._get_max_profit(wins_ALL))
        self.max_win_profit_LONG, self.max_win_profit_perc_LONG = (
            self._get_max_profit(wins_LONG))
        self.max_win_profit_SHORT, self.max_win_profit_perc_SHORT = (
            self._get_max_profit(wins_SHORT))
        
        self.min_win_profit_ALL, self.min_win_profit_perc_ALL = (
            self._get_min_profit(wins_ALL))
        self.min_win_profit_LONG, self.min_win_profit_perc_LONG = (
            self._get_min_profit(wins_LONG))
        self.min_win_profit_SHORT, self.min_win_profit_perc_SHORT = (
            self._get_min_profit(wins_SHORT))
        
        self.avg_win_profit_ALL, self.avg_win_profit_perc_ALL = (
            self._get_avg_profit(wins_ALL))
        self.avg_win_profit_LONG, self.avg_win_profit_perc_LONG = (
            self._get_avg_profit(wins_LONG))
        self.avg_win_profit_SHORT, self.avg_win_profit_perc_SHORT = (
            self._get_avg_profit(wins_SHORT))
        
        # Calculate max, min and avg profit values and percentages for losses
        self.max_loss_profit_ALL, self.max_loss_profit_perc_ALL = (
            self._get_min_profit(losses_ALL))
        self.max_loss_profit_LONG, self.max_loss_profit_perc_LONG = (
            self._get_min_profit(losses_LONG))
        self.max_loss_profit_SHORT, self.max_loss_profit_perc_SHORT = (
            self._get_min_profit(losses_SHORT))
        
        self.min_loss_profit_ALL, self.min_loss_profit_perc_ALL = (
            self._get_max_profit(losses_ALL))
        self.min_loss_profit_LONG, self.min_loss_profit_perc_LONG = (
            self._get_max_profit(losses_LONG))
        self.min_loss_profit_SHORT, self.min_loss_profit_perc_SHORT = (
            self._get_max_profit(losses_SHORT))
        
        self.avg_loss_profit_ALL, self.avg_loss_profit_perc_ALL = (
            self._get_avg_profit(losses_ALL))
        self.avg_loss_profit_LONG, self.avg_loss_profit_perc_LONG = (
            self._get_avg_profit(losses_LONG))
        self.avg_loss_profit_SHORT, self.avg_loss_profit_perc_SHORT = (
            self._get_avg_profit(losses_SHORT))

        # Calculate max, min and average SL hits in trading block
        SL_exits_ALL = results_ALL.query('exit_trigger == "STOP_LOSS"')
        SL_exits_LONG = SL_exits_ALL.query('direction_long == 1')
        SL_exits_SHORT = SL_exits_ALL.query('direction_long != 1')
        TSL_exits_ALL = results_ALL.query(
            'exit_trigger == "TRAILING_STOP_LOSS"')
        TSL_exits_LONG = TSL_exits_ALL.query('direction_long == 1')
        TSL_exits_SHORT = TSL_exits_ALL.query('direction_long != 1')
        TSL_win_exits_ALL = TSL_exits_ALL.query('net_profit >= 0')
        TSL_win_exits_LONG = TSL_win_exits_ALL.query('direction_long == 1')
        TSL_win_exits_SHORT = TSL_win_exits_ALL.query('direction_long != 1')
        TSL_loss_exits_ALL = TSL_exits_ALL.query('net_profit < 0')
        TSL_loss_exits_LONG = TSL_loss_exits_ALL.query('direction_long == 1')
        TSL_loss_exits_SHORT = TSL_loss_exits_ALL.query('direction_long != 1')
        EOTB_exits_ALL = results_ALL.query(
            'exit_trigger == "END_OF_TRADING_BLOCK"')
        EOTB_exits_LONG = EOTB_exits_ALL.query('direction_long == 1')
        EOTB_exits_SHORT = EOTB_exits_ALL.query('direction_long != 1')
        
        self.total_SL_exits_ALL = len(SL_exits_ALL.index)
        self.total_SL_exits_LONG = len(SL_exits_LONG.index)
        self.total_SL_exits_SHORT = len(SL_exits_SHORT.index)
        self.total_TSL_exits_ALL = len(TSL_exits_ALL.index)
        self.total_TSL_exits_LONG = len(TSL_exits_LONG.index)
        self.total_TSL_exits_SHORT = len(TSL_exits_SHORT.index)
        self.total_TSL_win_exits_ALL = len(TSL_win_exits_ALL.index)
        self.total_TSL_win_exits_LONG = len(TSL_win_exits_LONG.index)
        self.total_TSL_win_exits_SHORT = len(TSL_win_exits_SHORT.index)
        self.total_TSL_loss_exits_ALL = len(TSL_loss_exits_ALL.index)
        self.total_TSL_loss_exits_LONG = len(TSL_loss_exits_LONG.index)
        self.total_TSL_loss_exits_SHORT = len(TSL_loss_exits_SHORT.index)
        self.total_EOTB_exits_ALL = len(EOTB_exits_ALL.index)
        self.total_EOTB_exits_LONG = len(EOTB_exits_LONG.index)
        self.total_EOTB_exits_SHORT = len(EOTB_exits_SHORT.index)

        # self.max_SL_hits_in_trading_block_ALL = None
        # self.max_SL_hits_in_trading_block_LONG = None
        # self.max_SL_hits_in_trading_block_SHORT = None
        # self.min_SL_hits_in_trading_block_ALL = None
        # self.min_SL_hits_in_trading_block_LONG = None
        # self.min_SL_hits_in_trading_block_SHORT = None
        # self.avg_SL_hits_in_trading_block_ALL = None
        # self.avg_SL_hits_in_trading_block_LONG = None
        # self.avg_SL_hits_in_trading_block_SHORT = None

        # Calculate max drawdown
        self.max_drawdown = results_ALL['drawdown'].max()
        
        # Calculate profit factor
        losses_total_profit = losses_ALL['net_profit'].sum()
        if losses_total_profit:
            self.profit_factor = round(
                abs(wins_ALL['net_profit'].sum() / losses_total_profit), 3)
        else:
            self.profit_factor = 'undefined, no losses occurred'

        # Calculate total profit
        self.total_gross_profit = results_ALL['gross_profit'].sum()
        self.total_net_profit = results_ALL['net_profit'].sum()

        # Calculate total commission fees
        self.total_fees = results_ALL['fees'].sum()

        self.max_win_streak = 0
        self.max_loss_streak = 0
        self.total_breakeven_streak = 0

        # self.average_time_held_per_trade = 0
        # self.average_time_held_per_win = 0
        # self.average_time_held_per_loss = 0
        # self.average_time_held_per_breakeven = 0

        # self.min_time_held_for_any_trade = 0
        # self.min_time_held_for_winning_trade = 0
        # self.min_time_held_for_losing_trade = 0
        # self.min_time_held_for_breakeven_trade = 0

        # self.max_time_held_for_any_trade = 0
        # self.max_time_held_for_winning_trade = 0
        # self.max_time_held_for_losing_trade = 0
        # self.max_time_held_for_breakeven_trade = 0

        self._backtest_stats.loc[len(self._backtest_stats)] = ([
            self.name,
            self.symbol,
            self.backtest_start_time,
            self.backtest_end_time,
            self.entry_interval,
            self.trade_interval,
            self.params,
            self.risk_percentage,
            self.starting_capital,
            self.current_capital,
            self.total_gross_profit,
            self.total_fees,
            self.total_net_profit,
            self.profit_factor,
            self.max_drawdown,
            self.total_trades_ALL,
            self.total_wins_ALL,
            self.total_losses_ALL,
            self.max_win_streak,
            self.max_loss_streak,
            self.total_SL_exits_ALL,
            self.total_TSL_exits_ALL,
            self.total_EOTB_exits_ALL,
            self.avg_win_profit_perc_ALL,
            self.avg_loss_profit_perc_ALL,
            self.max_win_profit_perc_ALL,
            self.max_loss_profit_perc_ALL,
            self.min_win_profit_perc_ALL,
            self.min_loss_profit_perc_ALL]
        )

    def _get_max_profit(self, trades):
        '''
        Calculate the max profit from a dataframe of trades.

        The function gets the max profit value in the account currency
        and as a percentage of the account capital at the time of the trade.

        REQUIREMENTS
        ------------
        The trades dataframe must contain 'profit' and 'remaining_capital'
        columns, where 'profit' is the net profit of the trade entry and
        'remaining_capital' is the account capital remaining after the trade
        completed.

        PARAMETERS
        ----------
        trades: pandas.DataFrame
            A dataframe containing trades

        RETURNS
        -------
        max_profit: float
            The max profit value of all trades
        max_profit_percentage: float
            The max profit value as a percentage of the account capital at
            the time of the trade
        '''
        if len(trades.index) == 0:
            return None, None

        max_profit = trades['net_profit'].max()
        max_profit_percentage = None

        max_profit_trades = trades.query('net_profit == @max_profit')
        max_profit_percentage = round(
            (max_profit_trades.net_profit / (
                max_profit_trades.remaining_capital
                - max_profit_trades.net_profit
                )).max() * 100, 2)

        max_profit = round(max_profit, 2)
        return max_profit, max_profit_percentage

    def _get_min_profit(self, trades):
        '''
        Calculate the min profit from a dataframe of trades.

        The function gets the min profit value in the account currency
        and as a percentage of the account capital at the time of the trade.

        REQUIREMENTS
        ------------
        The trades dataframe must contain 'profit' and 'remaining_capital'
        columns, where 'profit' is the net profit of the trade entry and
        'remaining_capital' is the account capital remaining after the trade
        completed.

        PARAMETERS
        ----------
        trades: pandas.DataFrame
            A dataframe containing trades

        RETURNS
        -------
        min_profit: float
            The min profit value of all trades
        min_profit_percentage: float
            The min profit value as a percentage of the account capital at
            the time of the trade
        '''        
        if len(trades.index) == 0:
            return None, None

        min_profit = trades['net_profit'].min()
        min_profit_percentage = None

        min_profit_trades = trades.query('net_profit == @min_profit')
        min_profit_percentage = round(
            (min_profit_trades.net_profit / (
                min_profit_trades.remaining_capital
                - min_profit_trades.net_profit
                )).max() * 100, 2)

        min_profit = round(min_profit, 2)
        return min_profit, min_profit_percentage

    def _get_avg_profit(self, trades):
        '''
        Calculate the average profit from a dataframe of trades.

        The function gets the avg profit value in the account currency
        and as a percentage of the account capital at the time of the trade.

        REQUIREMENTS
        ------------
        The trades dataframe must contain 'profit' and 'remaining_capital'
        columns, where 'profit' is the net profit of the trade entry and
        'remaining_capital' is the account capital remaining after the trade
        completed.

        PARAMETERS
        ----------
        trades: pandas.DataFrame
            A dataframe containing trades

        RETURNS
        -------
        avg_profit_percentage: float
            The average profit value as a percentage of the account capital at
            the time of the trade
        '''
        if len(trades.index) == 0:
            return None, None
        
        avg_profit = round(trades['net_profit'].mean(), 2)
        avg_profit_percentage = None

        trade_profit_percentage = (
            (trades.net_profit / (
                trades.remaining_capital
                - trades.net_profit
                )) * 100)
        
        avg_profit_percentage = round(
            trade_profit_percentage.mean(), 2)

        return avg_profit, avg_profit_percentage
