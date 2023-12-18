import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import engine

def klines_plotter(engine, table_name, num_klines):

    # Read klines database into pandas df
    with engine.connect() as conn:
        df = pd.read_sql(table_name, conn)
    
    # Take last 20 minutes of data
    df = df.tail(num_klines)

    # Create plot
    plt.figure()

    # Set bar widths for candle body and wick
    body_width = 0.4
    wick_width = 0.05
    
    # Get separate dataframes for candles that went up and down
    up = df[df.close>=df.open]
    down = df[df.close<df.open]

    # Plot up prices as green
    plt.bar(up.index,up.close-up.open,body_width,bottom=up.open,color='green')
    plt.bar(up.index,up.high-up.low,wick_width,bottom=up.low,color='green')

    # Plot down prices as red
    plt.bar(down.index,down.close-down.open,body_width,bottom=down.open,color='red')
    plt.bar(down.index,down.high-down.low,wick_width,bottom=down.low,color='red')

    # Rotate x-axis tick labels
    plt.xticks(rotation=45, ha='right')

    # Display candlestick chart
    plt.show()