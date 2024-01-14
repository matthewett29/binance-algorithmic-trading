from datetime import datetime
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from .base import Base


class Kline(Base):

    __tablename__ = "klines"

    # Columns for storing the id, symbol and time interval
    # for a given entry of kline data
    id: Mapped[int] = mapped_column(primary_key=True)
    symbol: Mapped[str]
    interval: Mapped[str]

    # Columns for storing kline data received from Binance
    # See: https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data
    # for the latest kline data format
    open_time: Mapped[datetime]
    open: Mapped[float]
    high: Mapped[float]
    low: Mapped[float]
    close: Mapped[float]
    volume: Mapped[float]
    close_time: Mapped[datetime]
    quote_asset_volume: Mapped[float]
    num_trades: Mapped[int]
    taker_buy_base_asset_volume: Mapped[float]
    taker_buy_quote_asset_volume: Mapped[float]
