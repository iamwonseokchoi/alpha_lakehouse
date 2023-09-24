import os
import json
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

load_dotenv()

class PolygonNewsAPI:
    def __init__(self, tickers, start_date, api_key):
        self.base_url = "https://api.polygon.io/v2/reference/news"
        self.tickers = tickers
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.today()
        self.api_key = api_key
        self.news_data = []
        self.seen_ids = set()

    def fetch_news(self, ticker, from_date, to_date):
        params = {
            "ticker": ticker,
            "published_utc.gte": from_date,
            "published_utc.lte": to_date,
            "order": "asc",
            "limit": 1000,
            "sort": "published_utc",
            "apiKey": self.api_key
        }

        response = requests.get(self.base_url, params=params)
        if response.status_code == 200:
            json_data = response.json()
            for result in json_data["results"]:
                article_id = result.get("id")
                if article_id not in self.seen_ids:
                    self.news_data.append(result)
                    self.seen_ids.add(article_id)
                    
            next_url = json_data.get("next_url")
            while next_url:
                next_response = requests.get(next_url)
                if next_response.status_code == 200:
                    next_json_data = next_response.json()
                    for result in next_json_data["results"]:
                        article_id = result.get("id")
                        if article_id not in self.seen_ids:
                            self.news_data.append(result)
                            self.seen_ids.add(article_id)
                    next_url = next_json_data.get("next_url")
                else:
                    print(f"Failed to fetch next_url data for {ticker}, status code: {next_response.status_code}")
                    break
        else:
            print(f"Failed to fetch news data for {ticker}, status code: {response.status_code}")

    def fetch_for_ticker(self, ticker):
        curr_date = self.start_date
        delta = timedelta(days=7)  # 1 week time frame, adjust as needed

        while curr_date <= self.end_date:
            from_date = curr_date.strftime("%Y-%m-%d")
            to_date = (curr_date + delta).strftime("%Y-%m-%d")
            self.fetch_news(ticker, from_date, to_date)
            curr_date += delta + timedelta(days=1)

    def fetch_all_tickers(self):
        with ThreadPoolExecutor() as executor:
            executor.map(self.fetch_for_ticker, self.tickers)

if __name__ == "__main__":
    tickers = json.loads(os.getenv("TICKERS"))
    api_key = os.getenv("POLYGON_API_KEY")
    start_date = "2015-01-01"

    polygon_api = PolygonNewsAPI(tickers, start_date, api_key)
    polygon_api.fetch_all_tickers()
