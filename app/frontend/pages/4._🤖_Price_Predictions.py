import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from dotenv import load_dotenv
import json
import os

st.set_page_config(
    layout="wide"
)

st.title("Price Prediction")
st.subheader("Model: Stacked Bidirectional LSTM")

# Load Assets
load_dotenv()
tickers = json.loads(os.getenv('TICKERS', '[]'))

# Endpoints
ml_endpoint = "http://127.0.0.1:8000/ml/{}"
price_endpoint = "http://127.0.0.1:8000/cleaned/{}"

for ticker in tickers:
    try:
        with st.container():
            st.write('---')
            st.header(f"🔬 Price Prediction Trend: {ticker}")

            ml_response = requests.get(ml_endpoint.format(ticker))
            if ml_response.status_code != 200:
                st.error(f"Failed to retrieve data for {ticker}. Status code: {ml_response.status_code}")
                continue
            
            price_response = requests.get(price_endpoint.format(ticker))
            if price_response.status_code != 200:
                st.error(f"Failed to retrieve data for {ticker}. Status code: {price_response.status_code}")
                continue
            
            ml_data = ml_response.json()['data']
            df_ml = pd.DataFrame(ml_data)
            df_ml['timestamp'] = pd.to_datetime(df_ml['timestamp'])
            df_ml = df_ml.sort_values(by='timestamp', ascending=True)

            price_data = price_response.json()['data']
            df_price = pd.DataFrame(price_data)
            df_price['timestamp'] = pd.to_datetime(df_price['timestamp'])
            df_price = df_price.sort_values(by='timestamp', ascending=True)

            mean_prediction = df_ml['close'].mean()
            std_dev_prediction = df_ml['close'].std()
            prediction_range = df_ml['close'].max() - df_ml['close'].min()

            df_ml_grouped = df_ml.groupby('timestamp')['close'].agg(['mean', 'min', 'max']).reset_index()

            fig = go.Figure()

            fig.add_trace(go.Scatter(x=df_price['timestamp'], y=df_price['close'], mode='lines', name='Actual Price'))
            fig.add_trace(go.Scatter(x=df_ml_grouped['timestamp'], y=df_ml_grouped['mean'], mode='lines', name='Predicted Price'))

            fig.add_trace(go.Scatter(x=df_ml_grouped['timestamp'], y=df_ml_grouped['min'], fill=None, mode='lines', name='Lower Bound', line=dict(color='rgba(0,100,80,0.2)')))
            fig.add_trace(go.Scatter(x=df_ml_grouped['timestamp'], y=df_ml_grouped['max'], fill='tonexty', mode='lines', name='Upper Bound', line=dict(color='rgba(0,100,80,0.2)')))

            st.plotly_chart(fig, use_container_width=True)

            col1, col2, col3 = st.columns(3)
            
            col1.metric(label='Mean Prediction', value=f"{mean_prediction:.2f}")
            col2.metric(label='Std Dev Prediction', value=f"{std_dev_prediction:.2f}")
            col3.metric(label='Prediction Range', value=f"{prediction_range:.2f}")

    except Exception as e:
        st.error(f"An error occurred while processing {ticker}: {str(e)}")
