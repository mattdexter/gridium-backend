import os, logging, json, contextlib
import pandas as pd
import dateutil.parser

from scrapy import Spider
from scrapy.crawler import CrawlerProcess
from scrapy.http import Request

SCRAPY_USER_AGENT = os.getenv('SCRAPY_USER_AGENT')
SCRAPY_LOG_LEVEL = os.getenv('SCRAPY_LOG_LEVEL')
SCRAPY_FEED_FORMAT = os.getenv('SCRAPY_FEED_FORMAT')
SCRAPY_FEED_URI = os.getenv('SCRAPY_FEED_URI')

pd.set_option('display.width',5000)

# Scrapy spider for forecast data from tide-forecast.com
# Later, we invoke it programatically with the Scrapy API
class TideForecastSpider(Spider):
    name = "tide_forecast_spider"
    allowed_domains = ["tide-forecast.com"]
    start_urls = ["https://www.tide-forecast.com/"]

    # How to handle the initial response from the start URL.
    # We find the table of forecast page links and yield a new
    # request async'ly firing the next callback for each one.
    def parse(self, response):
        links = "//table[@class='list_table']//td"

        for link in response.xpath(links):
            item = dict()
            url = link.xpath("a/@href").extract_first()
            url = response.urljoin(url)
            title = link.xpath("a/text()").extract_first()
            item["url"] = url
            item["title"] = title

            yield Request(
                item["url"], self.parse_events,
                meta={"item": item})

    # How to handle the responses from each followed URL.
    # We procedurally populate the item's forecast data,
    # extracting selections with xpath as we loop rows.
    def parse_events(self, response):
        item = response.meta["item"]
        rows = "//table//tr"
        date = "td[@class='date']/text()"
        time = "td[@class='time ' or @class='time tide']/text()"
        meters = "td[@class='level metric']/text()"
        event = "td[last()]/text()"
        forecast, last_date = [], ""

        for row in response.xpath(rows):
            record = {
                "date": row.xpath(date).extract_first(
                    default=last_date),
                "time": row.xpath(time).extract_first(),
                "meters": row.xpath(meters).extract_first(),
                "event": row.xpath(event).extract_first(),
            }
            forecast.append(record)
            last_date = row.xpath(date).extract_first(
                default=last_date)

        timezone = "//table/tr/td[@class='time-zone']/text()"
        timezone = response.xpath(timezone).extract_first()

        item["forecast"] = forecast
        item["timezone"] = timezone
        yield item

        
# Clean up and format each item's scraped data.
# The crawler process uses this to write the data feed.
class TideForecastPipeline(object):
    def process_item(self, item, spider):
        for obj in item["forecast"]:
            date = obj["date"].strip()
            time = obj["time"].lstrip()

            datetime = "{} {}".format(date, time)
            datetime = dateutil.parser.parse(datetime)
            obj["datetime"] = datetime

            if obj["meters"]:
                meters = obj["meters"].replace(" m","")
                obj["meters"] = meters

        keys = ["datetime", "meters", "event"]
        forecast = [dict((k, obj[k]) for k in keys
                         if k in obj and obj[k])
                    for obj in item["forecast"]]

        item["forecast"] = forecast
        return item

    
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
              'app.TideForecastPipeline': 300,
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


if __name__ == "__main__":
    tcf = TideForecastChallenge()
    for d in tcf.data: print(d["query"])
