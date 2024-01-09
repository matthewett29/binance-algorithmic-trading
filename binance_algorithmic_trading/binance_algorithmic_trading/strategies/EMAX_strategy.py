from binance_algorithmic_trading.logger import Logger
from binance_algorithmic_trading.strategies.base_strategy import BaseStrategy


class EMAXStrategy(BaseStrategy):
    '''
    A basic EMA crossover strategy.

    Params
    ------
    EMA_fast: shorter period EMA
    EMA_slow: longer period EMA
    '''

    def __init__(self, log_level, database_manager):

        self._logger = Logger(logger_name=__file__,
                              log_level=log_level).get_logger()
        super().__init__(log_level, database_manager)
        self.name = "EMAX"

    def _execute_strategy(self):

        # Get strategy params
        params_list = self.params.split(":")
        self.EMA_fast_period = int(params_list[0])
        self.EMA_slow_period = int(params_list[1])

        # Get entry klines data from database
        entry_data = self._database_manager.get_klines_df(
            symbol=self.symbol,
            interval=self.entry_interval,
            start_time=self.backtest_start_time,
            end_time=self.backtest_end_time
        )

        # FIND ENTRY TRIGGERS
        # Add column for fast EMA values
        entry_data['EMA_fast'] = entry_data['close'].rolling(
            window=self.EMA_fast_period,
            closed='left'
        ).mean()

        # Add column for slow EMA values
        entry_data['EMA_slow'] = entry_data['close'].rolling(
            window=self.EMA_slow_period,
            closed='left'
        ).mean()

        # Add column for flag indicating fast EMA crossed above slow EMA
        entry_data['EMA_fast_above_slow'] = False
        entry_data.loc[
            entry_data.query('EMA_fast > EMA_slow').index,
            'EMA_fast_above_slow'
        ] = True

        # Add column for flag indicating when the fast and slow EMA cross
        # Note that diff() uses an XOR operation to check bool values so when
        # either the EMA_fast_above_slow changes from True to False or False
        # to True the results will be True
        entry_data['EMA_crossover'] = entry_data['EMA_fast_above_slow'].diff()

        # Cancel the first EMA crossover signal by setting it to be False
        # if the signal occurred after the EMA_slow value changed from NaN
        # to a value
        # Note this is required to avoid the false trigger when the slow EMA
        # value is first calculated during backtesting which, if lower than
        # the fast EMA value at the time, will trigger an EMA_crossover signal
        # because the EMA_fast_above_slow flag will have changed from False
        # (default initialisation) to True
        first_crossover_index = entry_data.query('EMA_crossover == 1').index[0]
        if entry_data['EMA_slow'].isna()[first_crossover_index-1]:
            entry_data.loc[
                first_crossover_index,
                'EMA_crossover'
            ] = False

        # Add column for flag indicating entry trigger
        # Enter when EMA crossover occurs and fast EMA is above slow EMA
        entry_data['entry_trigger'] = False
        entry_data.loc[
            entry_data.query(
                'EMA_crossover == 1 & EMA_fast_above_slow == 1').index,
            'entry_trigger'
        ] = True

        # FIND EXIT TRIGGERS
        # Add column for flag indicating exit trigger
        # Exit when EMA crossover occurs and fast EMA is below slow EMA
        entry_data['exit_trigger'] = False
        entry_data.loc[
            entry_data.query(
                'EMA_crossover == 1 & EMA_fast_above_slow == 0').index,
            'exit_trigger'
        ] = True

        # Remove all entry data except for when entry and exit triggers occur
        entry_and_exit_data = entry_data.query(
            'entry_trigger == 1 | exit_trigger == 1')

        highest_capital = self.starting_capital
        in_trade = False

        # Execute EMAX strategy
        for i, candle in entry_and_exit_data.iterrows():

            # Check for BUY
            if candle['entry_trigger'] and not in_trade:

                in_trade = True
                entry_time = candle['close_time']
                entry_price = candle['close']
                quantity = self.current_capital / entry_price

            # Check for SELL
            elif candle['exit_trigger'] and in_trade:

                in_trade = False
                exit_time = candle['close_time']
                exit_price = candle['close']
                gross_profit, net_profit, commission = self._calculate_profit(
                    quantity=quantity,
                    direction_long=True,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    use_BNB_for_commission=self.use_BNB_for_commission
                )

                self.current_capital += net_profit
                if self.current_capital > highest_capital:
                    drawdown = 0
                    highest_capital = self.current_capital
                else:
                    drawdown = round((
                        1 - (self.current_capital / highest_capital)
                    ) * 100, 2)

                self._log_trade(
                    symbol=self.symbol,
                    interval=self.entry_interval,
                    direction_long=True,
                    trade_block_id=None,
                    quantity=quantity,
                    entry_time=entry_time,
                    entry_price=entry_price,
                    stop_loss_price=None,
                    take_profit_price=None,
                    exit_trigger='EMA_CROSS_DOWN',
                    exit_time=exit_time,
                    exit_price=exit_price,
                    gross_profit=gross_profit,
                    net_profit=net_profit,
                    commission=commission,
                    remaining_capital=self.current_capital,
                    drawdown=drawdown
                )
