__author__ = 'Debanjan Mahata'

import time

from twython import Twython, TwythonError
from pymongo import MongoClient

import TwitterAuthentication as t_auth

def collect_tweets(path_name):

    try:
        #connecting to MongoDB database
        mongoObj = MongoClient()
        #setting the MongoDB database
        db = mongoObj["TwitterSentimentAnalysis"]
        #setting the collection in the database for storing the Tweets
        collection = db["sentiment_labeled_tweets"]
    except:
        print "Could not connect to the MongoDb Database, recheck the connection and the database"


    try:
        fp = open(path_name)
    except IOError:
        print "Please provide the right path to the file named labeledTweetSentimentCorpus.csv"

    request_count = 0
    key_count = 0

    auth_key = t_auth.keyList[key_count%11]

    for entry in fp:

        tweet_id = entry.rstrip().split(",")[2].replace('"',"")
        tweet_sentiment = entry.rstrip().split(",")[1].replace('"',"")

        twitter = Twython(auth_key["APP_KEY"],auth_key["APP_SECRET"],auth_key["OAUTH_TOKEN"],auth_key["OAUTH_TOKEN_SECRET"])

        if request_count == 1499:
            request_count = 0
            key_count += 1
            auth_key = t_auth.keyList[key_count%11]
            time.sleep(60)

        request_count += 1

        try:
            twitter_status = twitter.show_status(id = tweet_id)
            twitter_status["sentiment_label"] = tweet_sentiment
            collection.insert(twitter_status)
        except TwythonError:
            pass



if __name__ == "__main__":

    #call method for collecting and storing the tweets in a MongoDb collection
    collect_tweets("../CorpusAndLexicons/labeledTweetSentimentCorpus.csv")

