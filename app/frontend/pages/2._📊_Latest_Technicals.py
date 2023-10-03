import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
import json
import os

st.set_page_config(
    layout="wide"
)

st.title("Latest Technical Indicators")
st.subheader("1-Minute Aggregated: EMAs, MACD, RSI, SMAs")
st.write("Displaying 5 minutes of data")

# Load Assets
load_dotenv()
indicators = ["ema", "macd", "rsi", "sma"]
tickers = json.loads(os.getenv('TICKERS', '[]'))

# Endpoints
price_endpoint = "http://127.0.0.1:8000/technicals/{}/{}" # indicator, ticker

# Iterate through each indicator
for indicator in indicators:
    with st.container():
        st.write('---')
        st.header(f"📈 {indicator.upper()}")

        # Create a 2x2 grid for tickers
        col1, col2 = st.columns(2)
        col3, col4 = st.columns(2)

        column_mapping = {
            0: col1,
            1: col2,
            2: col3,
            3: col4
        }

        for idx, ticker in enumerate(tickers):
            try:
                col = column_mapping[idx % 4]
                response = requests.get(price_endpoint.format(indicator, ticker))
                if response.status_code != 200:
                    col.error(f"Failed to retrieve {indicator} data for {ticker}. Status code: {response.status_code}")
                    continue

                data = response.json()['data']
                df = pd.DataFrame(data)

                col.subheader(f"{ticker}")
                fig = px.line(df, x='timestamp', y="value", width=400, height=300)
                fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))  # Remove padding
                col.plotly_chart(fig, use_container_width=True)

            except Exception as e:
                col.error(f"An error occurred while processing {ticker} for {indicator}: {str(e)}")
