__author__ = 'Debanjan Mahata'

from pymongo import MongoClient



def no_distinct_entries(collection, field):
    return len(collection.distinct(field))

def no_documents(collection):
    return collection.count()


if __name__ == "__main__":

    try:
        #connecting to MongoDB database
        mongoObj = MongoClient()
        #setting the MongoDB database
        db = mongoObj["TwitterSentimentAnalysis"]
        #setting the collection in the database for storing the Tweets
        collection = db["emotion_labeled_tweets"]
    except:
        print "Could not connect to the MongoDb Database, recheck the connection and the database"

    print no_distinct_entries(collection, "id")
    print no_documents(collection)
