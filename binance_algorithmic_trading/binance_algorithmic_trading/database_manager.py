import json
from math import floor
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, select, inspect
from models.base import Base
from models.klines import Kline
from binance_algorithmic_trading.logger import Logger


class DatabaseManager():
    """
    A database manager class for storing and interacting with all app data.

    The database class provides an interface for creating a database based on
    the SQLAlchemy module, and requesting and storing data from the Binance API
    client.

    Future functions will enable storing all application related data such as
    configuration options, strategies, backtesting results, and live trading
    results.

    Attributes
    ----------
    None

    Methods
    -------
    __init__(log_level, config)
        Initialise the database engine based on the database models
    update_klines(symbols, intervals, client_manager)
        Update the database with klines data from the Binance client
    """

    _valid_symbols = [
        'BTCUSDT',
        'SHIBUSDT',
        'ETHUSDT'
    ]

    # See https://binance-docs.github.io/apidocs/delivery/en/
    _valid_intervals = [
        '1m',
        '3m',
        '5m',
        '15m',
        '30m',
        '1h',
        '2h',
        '4h',
        '6h',
        '8h',
        '12h',
        '1d',
        '3d',
        '1w',
        '1M'
    ]

    _seconds_per_interval = {
        'm': 60,
        'h': 60*60,
        'd': 24*60*60,
        'w': 7*24*60*60,
        'M': 28*24*60*60
    }

    def __init__(self, log_level, config):
        """
        Initialise the database engine and create the database connection.

        Parameter
        ---------
        log_level: int
            The logging level as specified in the logging module
        config: Dict
            Dictionary containing the SQLAlchemy database engine configuration
            URL which includes the database dialect, driver and file path
        """
        self._config = config
        self._logger = Logger(logger_name=__file__,
                              log_level=log_level).get_logger()

        try:
            self._engine = create_engine(self._config['DB_CONFIG'])
        except Exception as e:
            self._logger.critical("failed to initialise SQLAlchemy database "
                                  f"engine with error: {e}")
        else:
            self._logger.debug("initialised SQLAlchemy database engine")

        try:
            Base.metadata.create_all(self._engine)
        except Exception as e:
            self._logger.debug("failed to create database tables with error: "
                               f"{e}")
        else:
            self._logger.debug("created database tables: "
                               f"{inspect(self._engine).get_table_names()}")

    def update_klines(self, symbols, intervals, client_manager):
        """
        Update the database with klines data from Binance.

        The klines data to be updated consists of each symbol listed in the
        symbols parameter and, for each symbol, all intervals listed in the
        intervals parameter. The function 1) checks that each symbol and
        interval are valid options as described in the Binance API
        documentation, 2) checks the database for existing data for each symbol
        and interval pair, 3) fetches any earlier or later data that is missing
        and, 4) updates the database with the data.

        Note that no data is returned because the database is updated during
        the function execution.

        Parameters
        ----------
        symbols: List[str]
            List of crypto pair symbols for which klines data is to be updated
        intervals: List[str]
            List of timeframe intervals for which klines data is to be updated
        client_manager: ClientManager
            A custom class for interfacing with the Binance Spot API (client)

        Returns
        -------
        None
        """
        symbols = json.loads(symbols)
        intervals = json.loads(intervals)

        self._logger.info("updating database with new klines, please wait...")
        # For each symbol and interval, get missing klines data
        for symbol in symbols:

            # Make the symbol all uppercase as requiered by binance
            # Required because the configparser file forces all strings to
            # lowercase
            symbol = symbol.upper()

            # Check symbol is valid
            if symbol not in self._valid_symbols:
                self._logger.warning(f"skipping invalid symbol '{symbol}' "
                                     "listed in app.cfg file")
                continue

            for interval in intervals:
                # Check interval is valid
                if interval not in self._valid_intervals:
                    self._logger.warning("skipping invalid interval "
                                         f"'{interval}' listed in app.cfg "
                                         "file")
                    continue

                # Get missing klines data for given symbol and interval
                self._logger.debug("checking for new data for "
                                   f"'{symbol} {interval}'")
                self._get_missing_klines(symbol, interval, client_manager)
        
        self._logger.info("klines database is up to date")

    def _get_missing_klines(self, symbol, interval, client_manager):
        '''
        This function finds missing klines entries and calls _get_klines with
        the required end_time and reverse parameters to control which klines
        are retrieved

        Logic for the given symbol and interval:
        1. Check for first and last klines entry in the db
        2a. If last klines entry doesn't exist, then there are no klines
            entries in the db so all data needs retrieving
            2a.1. Get all klines from the current time, do this in reverse so
                  that the _get_klines function
                  returns as soon as no more klines are read
            2a.2 Now that klines data is in the db, get the last entry
        2b. If last klines entry does exist, then there are klines entries in
            the db
            2b.1 Get any klines data that exists prior to the oldest klines
                 entry in the db (this may happen if the app is closed while
                 2a.1 is occurring but yet to complete)
        3. Get any klines data that exists after the latest entry in the db
        '''

        # Check database for earliest klines entry for given symbol/interval
        self._logger.debug("checking latest klines data entry for "
                           f"'{symbol} {interval}'")
        with Session(self._engine) as session:
            # Get earliest (first) klines entry in the database
            first_klines_entry = session.scalars(
                select(Kline)
                .filter_by(symbol=symbol)
                .filter_by(interval=interval)
                .order_by(Kline.open_time)
            ).first()

        # No data exists
        if first_klines_entry is None:
            # Get all klines data before the current time
            self._logger.debug("no klines data exists for "
                               f"'{symbol} {interval}'")
            self._get_klines(
                symbol,
                interval,
                client_manager,
                end_time_ms_since_utc=datetime.now().timestamp()*1000,
                reverse=True
            )
        # klines data does exist
        else:
            # Get any klines data before the earliest entry
            # This is only required because if the app is closed while klines
            # data is being retrieved (in reverse) for a new symbol/interval,
            # then there may still be older klines data that was not retrieved
            self._logger.debug("checking for older klines data than what is "
                               f"in the database for '{symbol} {interval}'")

            # Get the open time of the klines entry one-prior to the earliest
            # entry that exists in the database
            open_time_of_next_earlier_entry = self._get_next_klines_start_time(
                last_time_ms_since_utc=(first_klines_entry
                                        .open_time.timestamp()*1000),
                interval=interval,
                klines_limit=1,
                reverse=True
            )

            # Get the klines data for all earlier entries
            self._get_klines(
                symbol,
                interval,
                client_manager,
                end_time_ms_since_utc=open_time_of_next_earlier_entry,
                reverse=True
            )

        # Get latest (last) klines entry in the database
        last_klines_entry = session.scalars(
            select(Kline)
            .filter_by(symbol=symbol)
            .filter_by(interval=interval)
            .order_by(Kline.open_time.desc())
        ).first()

        # Get remaining klines data up to the current time
        self._get_klines(
            symbol,
            interval,
            client_manager,
            end_time_ms_since_utc=last_klines_entry.open_time.timestamp()*1000,
            reverse=False
        )


    def _get_next_klines_start_time(self, last_time_ms_since_utc, interval, klines_limit, reverse=False):
    
        # Get the ms multiplier for a given interval
        interval_in_ms = (int(interval[:-1]) * self._seconds_per_interval[interval[-1]]) * 1000

        if not reverse:
            next_start_time_ms_since_utc = last_time_ms_since_utc + (interval_in_ms * klines_limit)
        else:
            next_start_time_ms_since_utc = last_time_ms_since_utc - (interval_in_ms * klines_limit)

        return next_start_time_ms_since_utc


    def _get_klines(self, symbol, interval, client_manager, end_time_ms_since_utc, reverse=False):
        # This is done in reverse (from most recent to most historic) such
        # that the _get_klines function can stop asking Binance for klines
        # data once it first receives null data, signifying the point at
        # which Binance began generating it. Otherwise, doing it forwards
        # would require knowing the datetime when the data began and is
        # less simple.

        # Get all klines from start_time_ms_since_utc until current time        
        klines_max_limit = 1500

        # if reverse, preset the start time to the end time that was passed in
        if reverse:
            start_time_ms_since_utc = end_time_ms_since_utc

        while True:

            if not reverse:
                # Set start time to the time of the next klines entry
                start_time_ms_since_utc = self._get_next_klines_start_time(
                    last_time_ms_since_utc=end_time_ms_since_utc,
                    interval=interval,
                    klines_limit=1
                ) 
                # Set end time to the time of the klines 1500 entries later
                end_time_ms_since_utc = self._get_next_klines_start_time(
                    last_time_ms_since_utc=start_time_ms_since_utc,
                    interval=interval,
                    klines_limit=klines_max_limit
                )

                # Break if new start time is past the current time
                current_time_ms_since_utc = floor(datetime.now().timestamp() * 1000)
                if start_time_ms_since_utc > current_time_ms_since_utc:
                    break

                # Limit end time to max current time        
                if end_time_ms_since_utc > current_time_ms_since_utc:
                    end_time_ms_since_utc = current_time_ms_since_utc
            else:
                # Set the end time to the previous start time 
                end_time_ms_since_utc = start_time_ms_since_utc

                # Set the start time to the time of the klines 1500 entries earlier
                start_time_ms_since_utc = self._get_next_klines_start_time(
                    last_time_ms_since_utc=start_time_ms_since_utc,
                    interval=interval,
                    klines_limit=klines_max_limit,
                    reverse=True
                )

            # take floor of start and end times to ensure when converted from ms to s there is no decimal values
            start_time_ms_since_utc = floor(start_time_ms_since_utc)
            end_time_ms_since_utc = floor(end_time_ms_since_utc)

            self._logger.debug(f"getting klines for '{symbol} {interval}' between {datetime.fromtimestamp(start_time_ms_since_utc/1000)} and {datetime.fromtimestamp(end_time_ms_since_utc/1000)}")

            # Get the klines between the start and end time from binance
            klines = client_manager.get_klines(
                symbol,
                interval,
                start_time=start_time_ms_since_utc,
                end_time=end_time_ms_since_utc,
                limit=klines_max_limit
            )

            # Check if klines exist (they don't if binance data doesn't exist for the given start and end times)
            if klines:
                self._save_klines(symbol, interval, klines)
            else:
                # Break as there are no more klines to read, finished getting all available historical data
                break

    def _save_klines(self, symbol, interval, klines):
        # Add klines to the database
        self._logger.debug(f"updating klines database with latest data for '{symbol} {interval}'")
        with Session(self._engine) as session:
            for kline in klines:
                new_kline_entry = Kline(
                    symbol=symbol,
                    interval=interval,
                    open_time=datetime.fromtimestamp(kline[0]/1000),
                    open=kline[1],
                    high=kline[2],
                    low=kline[3],
                    close=kline[4],
                    volume=kline[5],
                    close_time=datetime.fromtimestamp(kline[6]/1000),
                    quote_asset_volume=kline[7],
                    num_trades=kline[8],
                    taker_buy_base_asset_volume=kline[9],
                    taker_buy_quote_asset_volume=kline[10]
                )
                session.add(new_kline_entry)
            session.commit()