# Santal
## Daily Keyword Analysis on Twitter Data Pipeline
Santal is a data analytical pipeline built on top of Tweepy, AWS S3, Redshift and PySpark. Given a keyword and a date, Santal will pull data from Twitter using Tweepy API and store and raw JSON files in S3. Then the ETL process will extract, transform and load data into a data warehouse hosted on Redshift to enable further analysis.


## General Workflow
![Alt text](flowchart.jpg "Optional title")



1. User input keyword, and Santal will pull all related tweets, along with user and geo information using Tweepy API.
2. Raw data will be stored in Amazon S3 bucket in the format of JSON.
3. Copy data to staging tables in Redshift
4. ETL process that transform data into fact/dimension tables
5. Analytical queries and aggregate the data and perform analysis.


### Demo
[ChatGPT Analysis](notebooks/Analytics.ipynb)


