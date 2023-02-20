import tweepy
import os
import datetime
import json
import boto3
import io
import time


def upload_raw_data(key,data,s3_client,keyword,date):
    '''
    Upload data to S3 bucket
    '''

    #create in-memory JSON object
    buf = io.StringIO()


    # write each dictionary to a separate line in the buffer
    for item in data:
        
        #keyword for mapping table
        if key == 'tweets':
            item['keyword'] = keyword
        json.dump(item, buf)
        buf.write('\n')
    
    date = str(date.date())
    
    #upload
    response = s3_client.put_object(
        Bucket='santal', 
        Key=f'{key}/{keyword}/{date}.json',
        Body=buf.getvalue()
        )
    return response

def search_and_upload(keyword,date,tweepy_client,s3_client,max_page = 100):
    
    '''
    Searach a keyword for today and upload to s3
    
    '''
    tweet_fields = ['created_at','public_metrics','possibly_sensitive','text','id','geo']
    expansions = ['author_id','geo.place_id']
    place_fields = ['id','country','full_name','contained_within','place_type','geo']
    user_fields = ['description','id','location','name','verified','verified_type']
    
    
    query = f"{keyword} -is:retweet -is:reply lang:EN"
    
    pages = 0
    tweets = []
    users = []
    places = []
    
    #100 page * 100 result
    next_token = None
    
    while pages < max_page:
        print('Requesting page#',pages+1,'...')
        response = tweepy_client.search_recent_tweets(query=query,start_time = date,
                                 next_token = next_token,
                                 tweet_fields = tweet_fields,
                                 place_fields = place_fields,
                                 user_fields = user_fields,
                                 expansions=expansions,
                                 max_results=100)
        
        tweets+=response['data']
        users+=response['includes']['users']
        try:
            places+=response['includes']['places']
        except:
            pass
        
        pages+=1
        try:
            next_token = response['meta']['next_token']
        except:
            break
        
        #time.sleep(3)
    print('Finished Requesting data, total Results: ',len(tweets))


    upload_raw_data('tweets',tweets,s3_client,keyword,date)
    upload_raw_data('users',users,s3_client,keyword,date)
    upload_raw_data('places',places,s3_client,keyword,date)

    print('Finshied uploading to S3.')
