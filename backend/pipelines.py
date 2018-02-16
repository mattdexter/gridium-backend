import dateutil.parser

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
