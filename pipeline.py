from get_data import search_and_upload
from staging_data import stage_all_tables
from queries.etl_queries import *
from etl import perform_etl
import tweepy
import os
import boto3
import datetime

def populate_dwh(keyword,date,max_page):
    """
    Given a keyword and date, get all tweets from that date related to this keyword
    Upload JSON to S3 and ETL to Redshift 
    """

    #build tweepy client
    bearer_token = os.environ['TWEEPY_BEARER_TOKEN']
    tweepy_client = tweepy.Client(bearer_token=bearer_token,return_type=dict,wait_on_rate_limit=True)

    #build s3 client
    aws_key = os.environ['AWS_ACCESS_KEY']
    aws_secret = os.environ['AWS_ACCESS_SECRET']
    s3_client = boto3.client('s3',aws_access_key_id=aws_key,aws_secret_access_key=aws_secret)

    #get_tweets and upload
    search_and_upload(keyword,date,tweepy_client=tweepy_client,s3_client=s3_client,max_page=max_page)
    
    #redshift config
    redshift = {'DWH_DB_USER':os.environ['DWH_DB_USER'],
        'DWH_DB_PASSWORD':os.environ['DWH_DB_PASSWORD'],
        'DWH_ENDPOINT':os.environ['DWH_ENDPOINT'],
        'DWH_PORT':'5439',
        'DWH_DB':'dev',
        'DWH_IAM_ROLE': os.environ['DWH_IAM_ROLE']}

    #stage from s3 to redshift
    stage_all_tables(keyword,date,redshift)

    #ETL to datawarehouse
    perform_etl(redshift)