from datetime import datetime
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from typing import Optional
from .base import Base


class Backtest(Base):

    __tablename__ = "backtests"

    # Columns for storing the settings and performance stats for a backtest
    id: Mapped[int] = mapped_column(primary_key=True)
    strategy_name: Mapped[str]
    symbol: Mapped[str]
    start_time: Mapped[datetime]
    end_time: Mapped[datetime]
    entry_interval: Mapped[str]
    trade_interval: Mapped[Optional[str]]
    params: Mapped[Optional[str]]
    risk_percentage: Mapped[float]
    starting_capital: Mapped[float]
    ending_capital: Mapped[float]
    total_gross_profit: Mapped[float]
    total_fees: Mapped[float]
    total_net_profit: Mapped[float]
    profit_factor: Mapped[float]
    max_drawdown: Mapped[float]
    total_trades: Mapped[int]
    total_wins: Mapped[int]
    total_breakevens: Mapped[int]
    total_losses: Mapped[int]
    max_win_streak: Mapped[int]
    max_loss_streak: Mapped[int]
    total_SL_exits: Mapped[int]
    total_TSL_exits: Mapped[int]
    total_TP_exits: Mapped[int]
    total_EOT_exits: Mapped[int]
    avg_win_return: Mapped[float]
    avg_loss_return: Mapped[float]
    max_win_return: Mapped[float]
    max_loss_return: Mapped[float]
    min_win_return: Mapped[float]
    min_loss_return: Mapped[float]
