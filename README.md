# onlineretail
## This is a Work in progress

The basic idea:
* Parse the input file to build the Invoice objects, dealing with potential missing values in the lines
* Write the invoice object to a kafka topic
* Build a simple Kafka Streams app capable to:
  - consume the Invoice objects in the kafka topic
  - compute the 3 different aggregates:
    1. Count of distinct customer per country for each day
    1. Total revenues per day
    1. Sum of total distinct products sold per day
  - write these computed metrics in 3 different topic (one for each metric)
* Ingest messages of these topic in Apache Druid
  - refine metrics (i.e. we need to cut at a k-value the aggregates producted by at point 3.)
* Set up an Apache Superset dashboard to show graphically the computed metrics pointing at Apache Druid Dataset


The idea for the second item in the assignment is to integrate the existing flow with:
* a standalone application capable to produce Invoice events enriched with the new fields
* an Apache Streams to re-process old Invoice events in order to enrich these with the new fields. This component has to run once, just for enrich the previous produced messages.
* modify the existing Kafka Stream in order to compute the new metrics and adapt it to the new Invoice schema

![basic idea](https://github.com/antfranzoso/onlineretail/blob/master/diag.png?raw=true)

These assumption was made in the design of the solution:
* No continuos or periodic ingestion of historical events was meant, due the quite uncommon data interchange format of the previous loaded file. 
* Due to the merely demonstrative objective of the assignment no solutions were provided to production-related topics (compression, security, deployments schema, load, etc.)

These proposal could be evaluate for further work:
* Move all metrics computation to Apache Druid
* Add data validation checks for the incoming events (e.g. validate description product with a lookup table)
* Other metrics could be added:
  * top-K customers per month
  * new customer per day
