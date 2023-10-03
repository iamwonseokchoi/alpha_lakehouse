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


# Load Assets
load_dotenv()
tickers = json.loads(os.getenv('TICKERS', '[]'))
image = "https://assets5.lottiefiles.com/packages/lf20_fcfjwiyb.json"
latest_market_info = {}

# Endpoints
news_endpoint = "http://127.0.0.1:8000/news/{}"


# Page Title
st.title("📰 Latest News")


def load_lottieurl(url):
    r = requests.get(url)
    if r.status_code != 200:
        return None
    return r.json()


with st.container():    
    left_column, right_column = st.columns(2)
    with left_column: 
        st.header("Seeking α")
        st.write(f"""
            News is available from some popular sources with news filtered via Apache Spark.
            Currently displaying news items for : {', '.join(tickers)}
            """)
    
    with right_column:
        st_lottie(image, height=300, key="image")


try:
    for ticker in tickers:
        with st.container():
            st.write('---')
            st.header(f"Latest from: {ticker}")

            left_column, right_column = st.columns(2)

            response = requests.get(news_endpoint.format(ticker))
            if response.status_code != 200:
                st.error(f"Failed to retrieve data for {ticker}. Status code: {response.status_code}")
                continue

            data = response.json()['data']

            for item in data:
                logo_url = item['publisher']['logo_url']
                title = item['title']
                article_url = item['article_url']

                col1, col2 = st.columns([1, 4])

                with col1:
                    st.image(logo_url, width=75)

                with col2:
                    st.markdown(f"[{title}]({article_url})")


except Exception as e:
    st.error(f"An error occurred: {str(e)}")
