# gridium-backend
Hi Gridium, I found your other interview challenge on GitHub and gave it a try.

To set up and run:
- Clone and cd into the repository
- Run pip3 install -r requirements.txt
- Source the .env file
- Run python3 app.py

This uses Scrapy to scrape tide-forecast.com and Pandas to structure/query it.

I didn't do any front-end display for this one, just felt like working with the data.

I attempted to separate the concerns as below.

### Extraction concern (Scrapy):
- Scrapy invokes my custom scraper for tide-forecast.com
- The scraper crawls the front page's table of links & follows each

### Transformation concern (Scrapy pipeline):
- Resulting objects are saved to disk through a JSON pipeline
- Each thread of the crawler appends to a single JSONlines file
- TideForecastPipeline structures and transforms the data for storage

### Loading concern (TideForecastChallenge):
- JSONlines are loaded the the main thread in list comprehension
- We frame each item's forecast in a Pandas DataFrame
- We then group forecasted events & transpose them to columns
- We then zip the transposed values into a tuple of tuples
- Now the data's shorter, immutable, and better suited to analytic queries
- Serves kind of the same purpose as a SQL temp table

### Analytic concern (TideForecastChallenge):
- We now apply the specific query logic referenced in the requirements
- We split the tide columns into separate tuples-of-tuples for day & night
- We can do this by applying a custom lambda to the DataFrame on axis 1 (rows)
- The lambda tuple-comprehends the tide columns to filter nested tuples on day/night
- TideForecastChallenge is done instantiating; we now loop-print each query result
