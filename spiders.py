import os

from scrapy import Spider
from scrapy.http import Request

SCRAPY_USER_AGENT = os.getenv('SCRAPY_USER_AGENT')
SCRAPY_LOG_LEVEL = os.getenv('SCRAPY_LOG_LEVEL')
SCRAPY_FEED_FORMAT = os.getenv('SCRAPY_FEED_FORMAT')
SCRAPY_FEED_URI = os.getenv('SCRAPY_FEED_URI')

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
