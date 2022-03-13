from pymongo import MongoClient
import urllib
import time

CDH_VERSION = "CDH-7.1.8-1"


def updateToMongo(doc):
    url = "mongodb+srv://" + urllib.parse.quote_plus("pratyushbhatt1617") + ":" + urllib.parse.quote_plus(
        "Pratyush@123") + \
          "@cluster0.ckxbw.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"

    cluster = MongoClient(url)
    db = cluster["BenchmarkMain"]
    collection = db[CDH_VERSION]
    collection.insert_one(doc)


def retrieveDataFromMongo():
    url = "mongodb+srv://" + urllib.parse.quote_plus("pratyushbhatt1617") + ":" + urllib.parse.quote_plus(
        "Pratyush@123") + \
          "@cluster0.ckxbw.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"

    cluster = MongoClient(url)
    db = cluster["BenchmarkMain"]
    collection = db[CDH_VERSION]
    dict_array = collection.find().sort("time_stamp", -1).limit(15)
    print(dict_array)
    counter = 0
    for key in dict_array:
        if counter == 5:
            break
        if key['is_tera']:
            print(key)
            counter += 1
    # last_five_days_Data = []
    # for key in dict_array:
    #     lists = [key['teragen'], key['terasort'], key['teravalidate']]
    #     last_five_days_Data.append(lists)
    #
    # return last_five_days_Data


docs = {
    "teragen": {"one": "hey",
                "two": "bye bye",
                "three": "Hello"},
    "is_tera": True,
    "time_stamp": int(time.time())
}
docs2 = {
    "DFSIO": {"one": "hey1",
              "two": "bye bye1",
              "three": "Hello1"},
    "is_tera": False,
    "time_stamp": int(time.time())
}

updateToMongo(docs)
updateToMongo(docs2)
retrieveDataFromMongo()
