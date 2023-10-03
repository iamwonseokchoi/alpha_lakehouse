import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from st_aggrid import AgGrid
from dotenv import load_dotenv
import json
import os

st.set_page_config(
    layout="wide"
)

st.title("Latest Prices")

# Load Assets
load_dotenv()
tickers = json.loads(os.getenv('TICKERS', '[]'))

# Endpoints
price_endpoint = "http://127.0.0.1:8000/tables/{}"


for ticker in tickers:
    try:
        with st.container():
            st.write('---')
            st.header(f"💸 Latest Price Data: {ticker}")

            response = requests.get(price_endpoint.format(ticker))
            if response.status_code != 200:
                st.error(f"Failed to retrieve data for {ticker}. Status code: {response.status_code}")
                continue

            data = response.json()['data']
            df = pd.DataFrame(data)
            df = df.drop(columns=['symbol', 'updated_at', 'volume_weighted'])

            AgGrid(df)

            st.subheader("Close Price (Last 10 Intervals)")
            fig = px.line(df, x='timestamp', y="close", width=1100, height=500)
            fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"An error occurred while processing {ticker}: {str(e)}")
