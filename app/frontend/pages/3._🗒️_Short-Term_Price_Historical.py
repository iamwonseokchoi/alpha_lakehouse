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

st.title("Short-term Price Historical")
st.subheader("Hourly Aggregation Averages from 1-second Ticks")

# Load Assets
load_dotenv()
tickers = json.loads(os.getenv('TICKERS', '[]'))

# Endpoints
price_endpoint = "http://127.0.0.1:8000/cleaned/{}"

df_list = []
df_dict = {}

for ticker in tickers:
    try:
        response = requests.get(price_endpoint.format(ticker))
        if response.status_code != 200:
            st.error(f"Failed to retrieve data for {ticker}. Status code: {response.status_code}")
            continue

        data = response.json()['data']
        df = pd.DataFrame(data)

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(by='timestamp', ascending=True)

        df['ticker'] = ticker

        df_list.append(df)
        df_dict[ticker] = df

    except Exception as e:
        st.error(f"An error occurred while processing {ticker}: {str(e)}")

all_data = pd.concat(df_list, ignore_index=True)
st.write('---')
st.header("📋 Combined SR Historical Data")
combined_fig = px.line(all_data, x='timestamp', y='close', color='ticker', title=f'Close Prices for {", ".join(tickers)}')
st.plotly_chart(combined_fig, use_container_width=True)

for ticker, df in df_dict.items():
    with st.container():
        st.write('---')
        st.header(f"📋 SR Historical: {ticker}")

        fig = px.line(df, x='timestamp', y='close', title=f'Close Prices for {ticker}')
        st.plotly_chart(fig, use_container_width=True)

        col1, col2, col3 = st.columns(3)
        max_close = round(df['close'].max(), 2)
        min_close = round(df['close'].min(), 2)
        mean_close = round(df['close'].mean(), 2)
        
        change_from_mean_to_max = round(((max_close - mean_close) / mean_close) * 100, 2)

        difference_max_min = round(max_close - min_close, 2)
        difference_min_max = round(min_close - max_close, 2)  

        col1.metric(label='Max Close Price', value=f"${max_close}", delta=f"+${difference_max_min}")
        col2.metric(label='Min Close Price', value=f"${min_close}", delta=f"-${difference_min_max}")  
        col3.metric(label='Mean Close Price', value=f"${mean_close}", delta=f"{change_from_mean_to_max}%")
