# tasks


## requirements
- use Python 3.12
- use uv for package management
- use Makefile for task management
- use pytest for testing
- document available API options from BoardGameGeek API https://boardgamegeek.com/wiki/page/BGG_XML_API2
- use http://bgg.activityclub.org/bggdata/thingids.txt to retrieve universe of IDs for boardgames
- ensure processes conform to API limits as specified in the BoardGameGeek API documentation

## processes
- create process for requesting game info from BoardGameGeek API that conforms to the API limits
- create process for loading game info into a cloud data warehouse (Azure, BigQuery)
- create process for tracking requests and responses to ensure compliance with API limits
- create process for loading data into a data warehouse
- create process for updating data in the data warehouse
- create process for querying data from the data warehouse
- create process for visualizing data from the data warehouse
- create process for monitoring data quality in the data warehouse
- create process for a high level summary of the data in the data warehouse

## architecture
- fetch and store raw responses from BoardGameGeek API
- process raw responses and load to cloud data warehouse