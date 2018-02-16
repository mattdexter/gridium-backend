import os, json, contextlib
import pandas as pd

from .spiders import TideForecastSpider
from scrapy.crawler import CrawlerProcess

SCRAPY_USER_AGENT = os.getenv('SCRAPY_USER_AGENT')
SCRAPY_LOG_LEVEL = os.getenv('SCRAPY_LOG_LEVEL')
SCRAPY_FEED_FORMAT = os.getenv('SCRAPY_FEED_FORMAT')
SCRAPY_FEED_URI = os.getenv('SCRAPY_FEED_URI')

PANDAS_DISPLAY_WIDTH = int(os.getenv("PANDAS_DISPLAY_WIDTH"))
pd.set_option('display.width', PANDAS_DISPLAY_WIDTH)

# This will encapsulate the business logic for operating
# the spider crawler. Keeps it out of the main method
class TideForecastChallenge():
    def __init__(self):
        self.data = None
        self.update()

    # Run all the business logic needed to update the data
    # We keep each location's forecast in a dict called "data"
    def update(self):
        self.crawl_tide_forecast_spider()
        data = self.load_tide_forecast()
        for obj in data:
            obj = self.handle_tide_forecast(obj)
        self.data = data

    # Crawl the tide-forecast.com spider & pass the output
    # to a JSONlines file via the tide forecast pipeline
    def crawl_tide_forecast_spider(self):
        with contextlib.suppress(FileNotFoundError):
            os.remove(SCRAPY_FEED_URI)
        process = CrawlerProcess({
            "LOG_LEVEL": SCRAPY_LOG_LEVEL,
            "USER_AGENT": SCRAPY_USER_AGENT,
            "FEED_FORMAT": SCRAPY_FEED_FORMAT,
            "FEED_URI": SCRAPY_FEED_URI,
            "ITEM_PIPELINES": {
              'backend.pipelines.TideForecastPipeline': 300,
            }
        })
        process.crawl(TideForecastSpider)
        process.start()

    # Helper function to load & parse the line-oriented
    # data feed into the main thread's memory
    def load_tide_forecast(self):
        with open(SCRAPY_FEED_URI) as f:
            data = [json.loads(x.strip()) for x in f]
        return data

    # Helper function to frame, query, and console-print
    # query results for each location
    def handle_tide_forecast(self, obj):
        df = self.frame_tide_forecast(obj["forecast"])
        obj["query"] = self.query_tide_forecast(df)
        obj["forecast"] = df
        return obj

    # Structure the data for consumption by analytic queries
    # like the one required by the interview challenge
    def frame_tide_forecast(self, forecast):
            df = pd.DataFrame(forecast)

            datetime = pd.DatetimeIndex(df["datetime"])
            df = df.set_index(pd.MultiIndex.from_arrays(
                [datetime.date, datetime.time],
                names=["date","time"]))
            df = df.reset_index().set_index("date")
            df = df[["time","event","meters"]]

            is_sunrise = df["event"] == "Sunrise"
            is_sunset = df["event"] == "Sunset"
            is_low_tide = df["event"] == "Low Tide"
            is_high_tide = df["event"] == "High Tide"

            sunrise = df[is_sunrise].groupby("date")
            sunset = df[is_sunset].groupby("date")
            low_tides = df[is_low_tide].groupby("date")
            high_tides = df[is_high_tide].groupby("date")

            def earliest(x):
                return min(x["time"])

            def zip_tides(x):
                return tuple(zip(x["time"], x["meters"]))

            df["sunrise"] = sunrise.apply(earliest)
            df["sunset"] = sunset.apply(earliest)
            df["low_tides"] = low_tides.apply(zip_tides)
            df["high_tides"] = high_tides.apply(zip_tides)

            df = df[["sunrise","sunset","low_tides","high_tides"]]
            df = df.drop_duplicates()
            return df

    # Query the data according the the challenge criteria.
    # (Time & height of each low tide between sunrise and sunset)
    def query_tide_forecast(self, df):
        df["day_high_tides"] = df.apply(
            lambda x: tuple(
                t for t in x.high_tides
                if x.sunrise < t[0] < x.sunset), axis=1)
        df["day_low_tides"] = df.apply(
            lambda x: tuple(
                t for t in x.low_tides
                if x.sunrise < t[0] < x.sunset), axis=1)
        df["night_high_tides"] = df.apply(
            lambda x: tuple(
                t for t in x.high_tides
                if not x.sunrise < t[0] < x.sunset), axis=1)
        df["night_low_tides"] = df.apply(
            lambda x: tuple(
                t for t in x.low_tides
                if not x.sunrise < t[0] < x.sunset), axis=1)
        df = df[["sunrise", "sunset",
                 "day_high_tides","day_low_tides",
                 "night_high_tides","night_low_tides"]]
        return df
