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

        self.strategy_name = None

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
        self.total_SL_exits = 0
        self.total_TSL_exits = 0
        self.total_EOT_exits = 0
        self.avg_win_profit_perc = 0
        self.avg_loss_profit_perc = 0
        self.max_win_profit_perc = 0
        self.max_loss_profit_perc = 0
        self.min_win_profit_perc = 0
        self.min_loss_profit_perc = 0

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

        self._logger.info(f"starting backtest using {self.strategy_name} "
                          f"strategy between {start_time} and {end_time}")

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

        # Calculate and return stats for the strategy
        self._calculate_stats()
        return self.get_stats()

    def get_stats(self):
        return self._backtest_stats

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

        self.win_rate_short = None
        self.loss_rate_short = None
        self.breakeven_rate_short = None
        self.win_rate_long = None
        self.loss_rate_long = None
        self.breakeven_rate_long = None
        self.win_rate = None
        self.loss_rate = None
        self.breakeven_rate = None
        self.max_win_profit = None
        self.max_win_profit_perc = None
        self.max_win_profit_long = None
        self.max_win_profit_perc_long = None
        self.max_win_profit_short = None
        self.max_win_profit_perc_short = None
        self.min_win_profit = None
        self.min_win_profit_perc = None
        self.min_win_profit_long = None
        self.min_win_profit_perc_long = None
        self.min_win_profit_short = None
        self.min_win_profit_perc_short = None
        self.avg_win_profit = None
        self.avg_win_profit_perc = None
        self.avg_win_profit_long = None
        self.avg_win_profit_perc_long = None
        self.avg_win_profit_short = None
        self.avg_win_profit_perc_short = None
        self.max_loss_profit = None
        self.max_loss_profit_perc = None
        self.max_loss_profit_long = None
        self.max_loss_profit_perc_long = None
        self.max_loss_profit_short = None
        self.max_loss_profit_perc_short = None
        self.min_loss_profit = None
        self.min_loss_profit_perc = None
        self.min_loss_profit_long = None
        self.min_loss_profit_perc_long = None
        self.min_loss_profit_short = None
        self.min_loss_profit_perc_short = None
        self.avg_loss_profit = None
        self.avg_loss_profit_perc = None
        self.avg_loss_profit_long = None
        self.avg_loss_profit_perc_long = None
        self.avg_loss_profit_short = None
        self.avg_loss_profit_perc_short = None
        self.total_SL_exits = None
        self.total_SL_exits_long = None
        self.total_SL_exits_short = None
        self.total_TSL_exits = None
        self.total_TSL_exits_long = None
        self.total_TSL_exits_short = None
        self.total_TSL_win_exits = None
        self.total_TSL_win_exits_long = None
        self.total_TSL_win_exits_short = None
        self.total_TSL_loss_exits = None
        self.total_TSL_loss_exits_long = None
        self.total_TSL_loss_exits_short = None
        self.total_EOT_exits = None
        self.total_EOT_exits_long = None
        self.total_EOT_exits_short = None

        self.max_SL_hits_in_trading_block = None
        self.max_SL_hits_in_trading_block_long = None
        self.max_SL_hits_in_trading_block_short = None
        self.min_SL_hits_in_trading_block = None
        self.min_SL_hits_in_trading_block_long = None
        self.min_SL_hits_in_trading_block_short = None
        self.avg_SL_hits_in_trading_block = None
        self.avg_SL_hits_in_trading_block_long = None
        self.avg_SL_hits_in_trading_block_short = None

        results = self._trade_log
        results_long = results.query('direction_long == 1')
        results_short = results.query('direction_long == 0')

        # Get dataframes for wins, losses and breakevens
        wins = results.query('net_profit > 0')
        wins_long = wins.query('direction_long == 1')
        wins_short = wins.query('direction_long == 0') 
        losses = results.query('net_profit < 0')
        losses_long = losses.query('direction_long == 1')
        losses_short = losses.query('direction_long == 0') 
        breakevens = results.query('net_profit == 0')
        breakevens_long = breakevens.query('direction_long == 1')
        breakevens_short = breakevens.query('direction_long == 0') 

        # Calculate total trades
        self.total_trades = len(results.index)
        self.total_trades_long = len(results_long.index)
        self.total_trades_short = len(results_short.index)

        # Calculate total wins and losses
        self.total_wins = len(wins.index)
        self.total_wins_long = len(wins_long.index)
        self.total_wins_short = len(wins_short.index)
        self.total_losses = len(losses.index)
        self.total_losses_long = len(losses_long.index)
        self.total_losses_short = len(losses_short.index)
        self.total_breakevens = len(breakevens.index)
        self.total_breakevens_long = len(breakevens_long.index)
        self.total_breakevens_short = len(breakevens_short.index)

        # Calculate win, loss and breakeven rates
        if self.total_trades > 0:
            self.win_rate = round(
                self.total_wins / self.total_trades, 3)
            self.loss_rate = round(
                self.total_losses / self.total_trades, 3)
            self.breakeven_rate = round(
                self.total_breakevens / self.total_trades, 3)
            
        if self.total_trades_long > 0:
            self.win_rate_long = round(
                self.total_wins_long / self.total_trades_long, 3)
            self.loss_rate_long = round(
                self.total_losses_long / self.total_trades_long, 3)
            self.breakeven_rate_long = round(
                self.total_breakevens_long / self.total_trades_long, 3)

        if self.total_trades_short > 0:
            self.win_rate_short = round(
                self.total_wins_short / self.total_trades_short, 3)
            self.loss_rate_short = round(
                self.total_losses_short / self.total_trades_short, 3)
            self.breakeven_rate_short = round(
                self.total_breakevens_short / self.total_trades_short, 3)

        # Calculate max, min and avg profit values and percentages for wins
        self.max_win_profit, self.max_win_profit_perc = (
            self._get_max_profit(wins))
        self.max_win_profit_long, self.max_win_profit_perc_long = (
            self._get_max_profit(wins_long))
        self.max_win_profit_short, self.max_win_profit_perc_short = (
            self._get_max_profit(wins_short))
        
        self.min_win_profit, self.min_win_profit_perc = (
            self._get_min_profit(wins))
        self.min_win_profit_long, self.min_win_profit_perc_long = (
            self._get_min_profit(wins_long))
        self.min_win_profit_short, self.min_win_profit_perc_short = (
            self._get_min_profit(wins_short))
        
        self.avg_win_profit, self.avg_win_profit_perc = (
            self._get_avg_profit(wins))
        self.avg_win_profit_long, self.avg_win_profit_perc_long = (
            self._get_avg_profit(wins_long))
        self.avg_win_profit_short, self.avg_win_profit_perc_short = (
            self._get_avg_profit(wins_short))
        
        # Calculate max, min and avg profit values and percentages for losses
        self.max_loss_profit, self.max_loss_profit_perc = (
            self._get_min_profit(losses))
        self.max_loss_profit_long, self.max_loss_profit_perc_long = (
            self._get_min_profit(losses_long))
        self.max_loss_profit_short, self.max_loss_profit_perc_short = (
            self._get_min_profit(losses_short))
        
        self.min_loss_profit, self.min_loss_profit_perc = (
            self._get_max_profit(losses))
        self.min_loss_profit_long, self.min_loss_profit_perc_long = (
            self._get_max_profit(losses_long))
        self.min_loss_profit_short, self.min_loss_profit_perc_short = (
            self._get_max_profit(losses_short))
        
        self.avg_loss_profit, self.avg_loss_profit_perc = (
            self._get_avg_profit(losses))
        self.avg_loss_profit_long, self.avg_loss_profit_perc_long = (
            self._get_avg_profit(losses_long))
        self.avg_loss_profit_short, self.avg_loss_profit_perc_short = (
            self._get_avg_profit(losses_short))

        # Calculate max, min and average SL hits in trading block
        SL_exits = results.query('exit_trigger == "STOP_LOSS"')
        SL_exits_long = SL_exits.query('direction_long == 1')
        SL_exits_short = SL_exits.query('direction_long != 1')
        TSL_exits = results.query(
            'exit_trigger == "TRAILING_STOP_LOSS"')
        TSL_exits_long = TSL_exits.query('direction_long == 1')
        TSL_exits_short = TSL_exits.query('direction_long != 1')
        TSL_win_exits = TSL_exits.query('net_profit >= 0')
        TSL_win_exits_long = TSL_win_exits.query('direction_long == 1')
        TSL_win_exits_short = TSL_win_exits.query('direction_long != 1')
        TSL_loss_exits = TSL_exits.query('net_profit < 0')
        TSL_loss_exits_long = TSL_loss_exits.query('direction_long == 1')
        TSL_loss_exits_short = TSL_loss_exits.query('direction_long != 1')
        TP_exits = results.query(
            'exit_trigger == "TAKE_PROFIT"')
        TP_exits_long = TP_exits.query('direction_long == 1')
        TP_exits_short = TP_exits.query('direction_long != 1')
        EOTB_exits = results.query(
            'exit_trigger == "END_OF_TRADING_BLOCK"')
        EOTB_exits_long = EOTB_exits.query('direction_long == 1')
        EOTB_exits_short = EOTB_exits.query('direction_long != 1')
        
        self.total_SL_exits = len(SL_exits.index)
        self.total_SL_exits_long = len(SL_exits_long.index)
        self.total_SL_exits_short = len(SL_exits_short.index)
        self.total_TSL_exits = len(TSL_exits.index)
        self.total_TSL_exits_long = len(TSL_exits_long.index)
        self.total_TSL_exits_short = len(TSL_exits_short.index)
        self.total_TSL_win_exits = len(TSL_win_exits.index)
        self.total_TSL_win_exits_long = len(TSL_win_exits_long.index)
        self.total_TSL_win_exits_short = len(TSL_win_exits_short.index)
        self.total_TSL_loss_exits = len(TSL_loss_exits.index)
        self.total_TSL_loss_exits_long = len(TSL_loss_exits_long.index)
        self.total_TSL_loss_exits_short = len(TSL_loss_exits_short.index)
        self.total_TP_exits = len(TP_exits.index)
        self.total_TP_exits_long = len(TP_exits_long.index)
        self.total_TP_exits_short = len(TP_exits_short.index)
        self.total_EOT_exits = len(EOTB_exits.index)
        self.total_EOT_exits_long = len(EOTB_exits_long.index)
        self.total_EOT_exits_short = len(EOTB_exits_short.index)

        # self.max_SL_hits_in_trading_block = None
        # self.max_SL_hits_in_trading_block_long = None
        # self.max_SL_hits_in_trading_block_short = None
        # self.min_SL_hits_in_trading_block = None
        # self.min_SL_hits_in_trading_block_long = None
        # self.min_SL_hits_in_trading_block_short = None
        # self.avg_SL_hits_in_trading_block = None
        # self.avg_SL_hits_in_trading_block_long = None
        # self.avg_SL_hits_in_trading_block_short = None

        # Calculate max drawdown
        self.max_drawdown = results['drawdown'].max()

        # Update ending capital var
        self.ending_capital = self.current_capital
        
        # Calculate profit factor
        losses_total_profit = losses['net_profit'].sum()
        if losses_total_profit:
            self.profit_factor = round(
                abs(wins['net_profit'].sum() / losses_total_profit), 3)
        else:
            self.profit_factor = 'undefined, no losses occurred'

        # Calculate total profit
        self.total_gross_profit = results['gross_profit'].sum()
        self.total_net_profit = results['net_profit'].sum()

        # Calculate total commission fees
        self.total_fees = results['fees'].sum()

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

        self._backtest_stats = {
            'strategy_name': self.strategy_name,
            'symbol': self.symbol,
            'start_time': self.backtest_start_time,
            'end_time': self.backtest_end_time,
            'entry_interval': self.entry_interval,
            'trade_interval': self.trade_interval,
            'params': self.params,
            'risk_percentage': self.risk_percentage,
            'starting_capital': self.starting_capital,
            'ending_capital': self.ending_capital,
            'total_gross_profit': self.total_gross_profit,
            'total_fees': self.total_fees,
            'total_net_profit': self.total_net_profit,
            'profit_factor': self.profit_factor,
            'max_drawdown': self.max_drawdown,
            'total_trades': self.total_trades,
            'total_wins': self.total_wins,
            'total_breakevens': self.total_breakevens,
            'total_losses': self.total_losses,
            'max_win_streak': self.max_win_streak,
            'max_loss_streak': self.max_loss_streak,
            'total_SL_exits': self.total_SL_exits,
            'total_TSL_exits': self.total_TSL_exits,
            'total_TP_exits': self.total_TP_exits,
            'total_EOT_exits': self.total_EOT_exits,
            'avg_win_return': self.avg_win_profit_perc,
            'avg_loss_return': self.avg_loss_profit_perc,
            'max_win_return': self.max_win_profit_perc,
            'max_loss_return': self.max_loss_profit_perc,
            'min_win_return': self.min_win_profit_perc,
            'min_loss_return': self.min_loss_profit_perc
        }

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
