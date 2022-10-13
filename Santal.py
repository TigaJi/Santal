#import
#database connection


import tweepy #tweepy
import config #twitter developer account credential 
import requests #request news api and polygon

import time as t
from datetime import datetime
from datetime import timedelta

import pandas as pd

import string
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
# lemmatizer = WordNetLemmatizer()
#nltk.download('averaged_perceptron_tagger')
#nltk.download('omw-1.4')
from nltk.corpus import wordnet
from nltk.sentiment.vader import SentimentIntensityAnalyzer

class Santal:
    def __init__(self,client,tickers):
        self.client = client
        self.tickers = tickers
        self.since_date = (datetime.today()-timedelta(days = 1)).strftime('%Y-%m-%d')
        self.until_date = datetime.today().strftime('%Y-%m-%d')
        self.stopwords = stopwords.words('english')
        self.lemmatizer = WordNetLemmatizer()
        
    
    def pos_tagger(self,nltk_tag):
        if nltk_tag.startswith('J'):
            return wordnet.ADJ
        elif nltk_tag.startswith('V'):
            return wordnet.VERB
        elif nltk_tag.startswith('N'):
            return wordnet.NOUN
        elif nltk_tag.startswith('R'):
            return wordnet.ADV
        else:         
            return None
    
    def clean_tweet(self,tweet):
        #pos tagging
        tweet = nltk.pos_tag(nltk.word_tokenize(tweet)) 
        tweet = list(map(lambda x: [x[0], self.pos_tagger(x[1])], tweet))
        #tweet = nltk.word_tokenize(tweet)
        ptr = 0
        while ptr < (len(tweet)):
            #remove line breaker
            #remove tickers, @s,links, stopwords and numbers
            if tweet[ptr][0][:5] == 'https' or tweet[ptr][0] in self.stopwords or tweet[ptr][0][0] in string.punctuation or tweet[ptr][0][:2] == 'r/'or any(char.isdigit() for char in tweet[ptr][0]):
                if tweet[ptr][0][0] in ['@','$']:
                    tweet.pop(ptr)
                    if ptr < (len(tweet)):
                        tweet.pop(ptr)
                    ptr-=1
                else:
                    tweet.pop(ptr)
                    ptr-=1

            #decapitalization & lemmatize
            if ptr < (len(tweet)):
                try:
                    tweet[ptr][0] = tweet[ptr][0].lower()
                    if tweet[ptr][1] == None:
                        tweet[ptr] = tweet[ptr][0]
                    else:
                        tweet[ptr] = self.lemmatizer.lemmatize(tweet[ptr][0],tweet[ptr][1])
                except Exception as e:
                    pass
                ptr+=1

        return ' '.join(tweet)
    
    def get_sentiment_score(self,text):
        sid = SentimentIntensityAnalyzer()
        if text == None:
            score = 0.0
        else:
            score = sid.polarity_scores(text)['compound']
        return score
    
        
    def get_by_ticker(self,keyword,db,api):
    
    
        #establish db connection
        collection = db["tweets"]
    
        #establish query
        #exclude retweet, must be in english 
        query = "{} -RT lang:en since:{} until:{}".format("$"+keyword,self.since_date,self.until_date)
        page_num = 10000
        max_result = 100
        p = 1
        for page in tweepy.Cursor(api.search_tweets,q = query,tweet_mode = 'extended', count = max_result).pages(page_num):
            #print("page#",p)
            p+=1
            page_df = pd.DataFrame(columns = ['dt','text','retweet_cnt','favorite_cnt','score'],dtype=object)
            for tweet in page:
                #turn to json format
                tweet = tweet._json

                #format datetime
                dt = datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')

                #get info
                retweet_count = tweet['retweet_count']
                favorite_count = tweet['favorite_count']
                if tweet['truncated'] == True:
                    text = tweet['extended_tweet']['full_text']
                else:
                    text = tweet['full_text']
                
                score = self.get_sentiment_score(self.clean_tweet(text))
                
                page_df.loc[len(page_df)]=[dt,text,retweet_count,favorite_count,score]
            page_df.reset_index(level=0, drop = True, inplace=True)

            collection.insert_many(page_df.to_dict('records'))
            
    def clear_history(self):
        for ticker in self.tickers:
            self.client.drop_database(ticker)
        
        
    def get(self):
        
        auth = tweepy.OAuthHandler(consumer_key = config.consumer_key,  consumer_secret = config.consumer_secret)
        auth.set_access_token(config.access_token,config.access_token_secret)

        api = tweepy.API(auth,wait_on_rate_limit = True)
        
        self.clear_history()
        
        for ticker in self.tickers:
            db = self.client[ticker]
            print("Getting: " + ticker)
            self.get_by_ticker(ticker,db,api)
        
        print("done")
        
           
        
        
    def analyze(self):
        res_df = pd.DataFrame(columns = ['ticker','# of tweets','Avg.score'])
        
        for i in range(len(self.tickers)):
            
            
            ticker = self.tickers[i]
            
            db = self.client[ticker]
            tweets = db["tweets"]
            tweets_df = pd.DataFrame(list(tweets.find()))
            
            count = len(tweets_df)
            avg_score = tweets_df['score'].mean()
            res_df.loc[i] = [ticker,count,avg_score]
        
        print(res_df)
         