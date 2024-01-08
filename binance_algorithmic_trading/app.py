#!python3
import logging
from datetime import datetime, timedelta
from binance_algorithmic_trading.logger import Logger
from binance_algorithmic_trading.config import get_config
from binance_algorithmic_trading.database_manager import DatabaseManager
from binance_algorithmic_trading.client_manager import ClientManager
from binance_algorithmic_trading.strategies.proprietary.JITF import JumpInTheFlowStrategy

def main():
    '''
    Description

    Summary

    Args

    '''
    # Get config options
    config = get_config()

    # # Initialise binance API client manager and connect
    # # using the API key/secret from app.cfg
    # client_manager = ClientManager(
    #     log_level=logging.DEBUG,
    #     config=config['binance']
    # )

    # # Get binance exchange info
    # client_manager.get_exchange_info()

    # Initialise database
    database_manager = DatabaseManager(
        log_level=logging.DEBUG,
        config=config['database']
    )

    # # Update database with all new data from binance
    # # for the symbols and intervals enabled in the config file
    # database_manager.update_klines(
    #     symbols=config['binance']['SYMBOLS'],
    #     intervals=config['binance']['INTERVALS'],
    #     client_manager=client_manager
    # )

    # Initialise Jump In The Flow Strategy
    strategy = JumpInTheFlowStrategy(log_level=logging.DEBUG)

    end_time = datetime.strptime("2024-01-08 09:35:00", "%Y-%m-%d %H:%M:%S")
    # end_time= datetime.now()

    # Backtest the strategy on all symols in app.cfg
    strategy.backtest(
        starting_capital=10000,
        risk_percentage=1,
        symbols=config['binance']['SYMBOLS'],
        start_time=end_time-timedelta(days=1825),
        end_time=end_time,
        entry_interval='12h',
        trade_interval='30m',
        N=10,
        X=0.2,
        Y=0.55,
        use_BNB_for_commission=True,
        database_manager=database_manager
    )

    # Get the strategy backtest stats
    stats = strategy.get_stats()
    stats.to_html('data/backtest_stats.html')


if __name__ == "__main__":
    # Initialise logger
    logger = Logger(logger_name=__file__, log_level=logging.DEBUG).get_logger()

    # Run main and allow stopping with CTRL-C (KeyboardInterrupt)
    try:
        logger.info("application started")
        main()
    except KeyboardInterrupt:
        logger.info("keyboard interrupt detected, exiting application")
