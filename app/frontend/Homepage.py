import streamlit as st
import requests
import json
from streamlit_lottie import st_lottie
import threading
from datetime import datetime
import pytz
from dotenv import load_dotenv
import os
import time
import pytz


# Streamlit code
st.set_page_config(
    page_title="α Lakehouse",
    page_icon="🚀",
    layout="wide"
)


# Load Assets
tickers = ["AAPL", "AMZN", "GOOGL", "MSFT", "NVDA", "TSLA"]
latest_market_info = {}

# Endpoints
market_endpoint = "http://127.0.0.1:8000/market"
ws_endpoint = "http://127.0.0.1:8000/websocket/{}"


# Page Title
st.title("α - Lakehouse")
st.sidebar.success("Select a category above to view")


def update_market_info(ticker):
    global latest_market_info
    max_retries = 3
    retry_count = 0

    while True:
        try:
            response = requests.get(ws_endpoint.format(ticker))
            if response.status_code == 200:
                latest_market_info[ticker] = response.json()['data']
                retry_count = 0 
            else:
                print(f"Failed to retrieve market info for {ticker}. Status code: {response.status_code}")
                
            time.sleep(10)
            
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            retry_count += 1
            if retry_count >= max_retries:
                print("Max retries reached. Stopping thread for ticker:", ticker)
                break

            time.sleep(2 ** retry_count) 

def format_price_delta(delta):
    return f"${delta:.2f}"

def format_non_price_delta(delta):
    return f"{delta:,}"


for ticker in tickers:
    threading.Thread(target=update_market_info, args=(ticker,)).start()

st.write(f"Real-time Prices")
st.write('---')
try:
    for ticker in tickers:
        with st.container():
            response = requests.get(ws_endpoint.format(ticker))
            if response.status_code != 200:
                st.error(f"Failed to retrieve data for {ticker}. Status code: {response.status_code}")
                continue

            data = response.json()['data']
            
            if ticker in latest_market_info and len(latest_market_info[ticker]) >= 2:
                latest = latest_market_info[ticker][0]
                prev = latest_market_info[ticker][1]

                close_delta = latest['close'] - prev['close']
                volume_delta = latest['volume'] - prev['volume']
                trades_delta = latest['total_trades'] - prev['total_trades']

                col1, col2, col3, col4 = st.columns(4)
                
                col1.title(f"{ticker}")
                col2.metric(label="Close Price", value=f"${latest['close']:.2f}", delta=format_price_delta(close_delta))
                col3.metric(label="Volume", value=f"{latest['volume']:,}", delta=format_non_price_delta(volume_delta))
                col4.metric(label="Total Trades", value=f"{latest['total_trades']:,}", delta=format_non_price_delta(trades_delta))

    # To auto-refresh the page every N seconds
    time.sleep(10)
    st.rerun()

except Exception as e:
    st.error(f"An error occurred: {str(e)}")




# import streamlit as st
# import requests
# from datetime import datetime
# from dotenv import load_dotenv
# import os
# import pytz
# from streamlit_autorefresh import st_autorefresh

# # Streamlit code
# st.set_page_config(
#     page_title="α Lakehouse",
#     page_icon="🚀",
#     layout="wide"
# )

# # Initialize session_state if it hasn't been initialized yet
# if 'last_fetched_values' not in st.session_state:
#     st.session_state.last_fetched_values = {}

# # Load Assets
# tickers = ["AAPL", "AMZN", "GOOGL", "MSFT", "NVDA", "TSLA"]
# latest_market_info = {}

# # Endpoints
# market_endpoint = "http://127.0.0.1:8000/market"
# ws_endpoint = "http://127.0.0.1:8000/websocket/{}"

# # Page Title
# st.title("α - Lakehouse")
# st.sidebar.success("Select a category above to view")

# # Refresh Component, set to update every 5 seconds
# st_autorefresh(interval=5 * 1000, key="dataframerefresh")

# # Functions to update market info and format deltas
# def update_market_info(ticker):
#     try:
#         response = requests.get(ws_endpoint.format(ticker))
#         if response.status_code == 200:
#             latest_market_info[ticker] = response.json()['data']
#         else:
#             st.error(f"Failed to retrieve data for {ticker}. Status code: {response.status_code}")
#     except Exception as e:
#         st.error(f"An error occurred: {str(e)}")

# def format_price_delta(delta):
#     return f"${delta:.2f}"

# def format_non_price_delta(delta):
#     return f"{delta:,}"

# # Main logic
# try:
#     for ticker in tickers:
#         # Initialize last fetched values for new tickers
#         if ticker not in st.session_state.last_fetched_values:
#             st.session_state.last_fetched_values[ticker] = {'close': None, 'volume': None, 'total_trades': None}
        
#         update_market_info(ticker)
#         data = latest_market_info[ticker][-1]  # Get the last dictionary from the list

#         # Calculate deltas
#         last_values = st.session_state.last_fetched_values[ticker]
#         close_delta = data['close'] - last_values['close'] if last_values['close'] else 0
#         volume_delta = data['volume'] - last_values['volume'] if last_values['volume'] else 0
#         trades_delta = data['total_trades'] - last_values['total_trades'] if last_values['total_trades'] else 0

#         # Update last fetched values in session_state
#         st.session_state.last_fetched_values[ticker] = {'close': data['close'], 'volume': data['volume'], 'total_trades': data['total_trades']}

#         col1, col2, col3, col4 = st.columns(4)
        
#         col1.title(f"{ticker}")
#         col2.metric(label="Close Price", value=f"${data['close']:.2f}", delta=format_price_delta(close_delta))
#         col3.metric(label="Volume", value=f"{data['volume']:,}", delta=format_non_price_delta(volume_delta))
#         col4.metric(label="Total Trades", value=f"{data['total_trades']:,}", delta=format_non_price_delta(trades_delta))

# except Exception as e:
#     st.error(f"An error occurred: {str(e)}")
