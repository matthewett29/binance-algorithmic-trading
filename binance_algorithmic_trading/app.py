#!python3
import logging
from pprint import pprint
from binance_algorithmic_trading.logger import Logger
from binance_algorithmic_trading.config import get_config
from binance_algorithmic_trading.database_manager import DatabaseManager
from binance_algorithmic_trading.client_manager import ClientManager


def main():
    '''
    Description

    Summary

    Args

    '''
    # Get config options
    config = get_config()

    # Initialise binance API client manager and connect
    # API key/secret are required for user data endpoints
    client_manager = ClientManager(
        log_level=logging.DEBUG,
        config=config['binance']
    )

    # Get binance exchange information for 'BTCUSDT'
    # _ = client_manager.get_exchange_info(symbols=['BTCUSDT'])

    # Initialise database
    database_manager = DatabaseManager(
        log_level=logging.DEBUG,
        config=config['database']
    )

    # Update database with all new data from binance
    # for the symbols and intervals enabled in the config file
    database_manager.update_klines(
        symbols=config['symbols'],
        intervals=config['intervals'],
        client_manager=client_manager
    )


if __name__ == "__main__":
    # Initialise logger
    logger = Logger(logger_name=__file__, log_level=logging.DEBUG).get_logger()

    # Run main and allow stopping with CTRL-C (KeyboardInterrupt)
    try:
        logger.info("application started")
        main()
    except KeyboardInterrupt:
        logger.info("keyboard interrupt detected, exiting application")
