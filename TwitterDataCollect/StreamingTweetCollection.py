__author__ = 'Debanjan Mahata'

from twython import TwythonStreamer
from TwitterAuthentication import keyList
from time import sleep
from random import randint
import sys,codecs

# from pymongo import MongoClient
# #connecting to MongoDB database
# mongoObj = MongoClient()
# #setting the MongoDB database
# db = mongoObj[]
# #setting the collection in the database for storing the Tweets
# collection = db[]


sys.stdout = codecs.lookup('iso8859-1')[-1](sys.stdout)

class MyStreamer(TwythonStreamer):

    def on_success(self, data):
        if data["lang"] == "en":
            print data["text"]
            # Want to disconnect after the first result?
            # self.disconnect()


    def on_error(self, status_code, data):
        sleep(randint(1,60))
        keys = keyList[randint(0,10)]
        stream = MyStreamer(keys["APP_KEY"],keys["APP_SECRET"],keys["OAUTH_TOKEN"],keys["OAUTH_TOKEN_SECRET"])
        stream.statuses.filter(track="#recipe")

## Requires Authentication as of Twitter API v1.1
while True:
    try:
        keys = keyList[randint(0,10)]
        stream = MyStreamer(keys["APP_KEY"],keys["APP_SECRET"],keys["OAUTH_TOKEN"],keys["OAUTH_TOKEN_SECRET"])
        stream.statuses.filter(track='#MissUSA')
    except:
        continue












