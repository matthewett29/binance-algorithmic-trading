#!python3
import logging
from pprint import pprint
from datetime import datetime, timedelta
from binance_algorithmic_trading.logger import Logger
from binance_algorithmic_trading.config import get_config
from binance_algorithmic_trading.database_manager import DatabaseManager
from binance_algorithmic_trading.client_manager import ClientManager
from binance_algorithmic_trading.strategies.EMAX_strategy import EMAXStrategy


def main():
    '''
    Description

    Summary

    Args

    '''
    # Get config options
    config = get_config()

    # Initialise binance API client manager and connect
    # using the API key/secret from app.cfg
    client_manager = ClientManager(
        log_level=logging.DEBUG,
        config=config['binance']
    )

    # Get binance exchange info
    client_manager.get_exchange_info()

    # Initialise database
    database_manager = DatabaseManager(
        log_level=logging.DEBUG,
        config=config['database']
    )

    # Update database with all new data from binance
    # for the symbols and intervals enabled in the config file
    database_manager.update_klines(
        symbols=config['binance']['SYMBOLS'],
        intervals=config['binance']['INTERVALS'],
        client_manager=client_manager
    )

    # Configure start and end time for backtesting
    end_time = datetime.now()
    start_time = end_time - timedelta(days=3650)

    # Initialise EMA Crossover Strategy
    EMA_fast = 20
    EMA_slow = 50
    params = f"{EMA_fast}:{EMA_slow}"
    strategy = EMAXStrategy(
        log_level=logging.DEBUG,
        database_manager=database_manager,
        params=params
    )

    # Backtest the strategy on all symbols in app.cfg
    results = strategy.backtest(
        starting_capital=10000,
        risk_percentage=100,
        symbols=config['binance']['SYMBOLS'],
        start_time=start_time,
        end_time=end_time,
        entry_interval='12h',
        trade_interval='30m',
        use_BNB_for_commission=True
    )

    # Save the backtest results
    database_manager.save_backtest(results)

    # Save the backtest trade log to file for review
    trade_log = strategy.get_trade_log()
    trade_log.to_html('data/backtest_trades.html')

    pprint(results, sort_dicts=False)


if __name__ == "__main__":
    # Initialise logger
    logger = Logger(logger_name=__file__, log_level=logging.DEBUG).get_logger()

    # Run main and allow stopping with CTRL-C (KeyboardInterrupt)
    try:
        logger.info("application started")
        main()
    except KeyboardInterrupt:
        logger.info("keyboard interrupt detected, exiting application")
