import sys

from pymongo import MongoClient
import urllib
import re

CDH_VERSION = "7.1.8"
tera_keys = ['teragen', 'terasort', 'teravalidate']
dfsio_keys = ['write', 'read']


# Converting the time into seconds and presentable form
def getTimeTaken(array, properties):  # done
    y_arr = []
    # print("Inside getTimeTaken", tera_array)
    for data in array:
        y = dict()
        for p in properties:
            if p in data:
                temp = re.findall(r'\d+', data[p])
                # print(temp)
                result = 0
                for i, value in reversed(list(enumerate(temp))):
                    result += (int(value) * (60 ** (len(temp) - i - 1)))
                #     print(i,value)
                y[p] = result
            else:
                y[p] = 0

        y_arr.append(y)

    return y_arr


def retrieveDataFromMongo(is_tera, FS, keys):
    url = "mongodb+srv://" + urllib.parse.quote_plus("pratyushbhatt1617") + ":" + urllib.parse.quote_plus(
        "Pratyush@123") + \
          "@cluster0.ckxbw.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"

    cluster = MongoClient(url)
    db = cluster["Benchmark2"]
    collection = db[CDH_VERSION]
    print("Hello")
    dict_array = collection.find().sort("time_stamp", -1).limit(20)
    # print(dict_array)
    last_five_days_Data = []
    counter = 0
    for key in dict_array:
        if counter == 5:
            break
        if key['is_tera'] == is_tera and key['file_system'] == FS:
            lists = []
            for k in keys:
                lists.append(key[k])
            # lists = [key['teragen'], key['terasort'], key['teravalidate']]
            last_five_days_Data.append(lists)
            counter += 1

    # print(last_five_days_Data)
    return last_five_days_Data


def getElapsedTime(array):
    elapsed_time = []
    for day in array:
        elapsed_time_day = []
        for job in day:
            elapsed = job['Elapsed:']
            elapsed_time_day.append(elapsed)

        elapsed_time.append(elapsed_time_day)

    return elapsed_time


def compareWithLastRun(et_data, keys):
    latest = et_data[0]
    previous_day = et_data[1]
    print(latest, previous_day)
    performance = dict()

    for i in range(len(latest)):
        perf = round(((latest[i] - previous_day[i]) / previous_day[i]) * 100, 2)
        # print(perf)
        performance[keys[i]] = perf

    return performance


def compareWithPastRuns(et_data):
    # print(len(et_data[0]))
    mini = [sys.maxsize] * len(et_data[0])
    maxi = [-sys.maxsize - 1] * len(et_data[0])
    for i in range(1,len(et_data)):
        for j in range(len(et_data[i])):
            mini[j] = min(mini[j], et_data[i][j])
            maxi[j] = max(maxi[j], et_data[i][j])

    print(mini)
    print(maxi)


def main():
    __is_tera = True
    FS = "HDFS"
    keys = tera_keys if __is_tera == True else dfsio_keys
    suite = "Terasuite" if __is_tera == True else "DFSIO"
    last_five_days = retrieveDataFromMongo(__is_tera, FS, keys)
    properties = ['Elapsed:', 'Average Map Time', 'Average Shuffle Time', 'Average Merge Time', 'Average Reduce Time']
    print(len(last_five_days), last_five_days)
    all_five_day_result = []
    for day in last_five_days:
        result = getTimeTaken(day, properties)
        all_five_day_result.append(result)
    for day in all_five_day_result:
        print(day)

    all_five_days_elapsed_time = getElapsedTime(all_five_day_result)
    for et in all_five_days_elapsed_time:
        print(et)

    performance = compareWithLastRun(all_five_days_elapsed_time, keys)
    print(performance)
    result = f"Summary: {suite} in {FS}, latest run vs last run:\n"
    for k in performance.keys():
        status = "down" if performance[k] < 0 else "up"
        result += f"{k.title()} is {performance[k]}% {status.title()}\n"
    print(result)

    compareWithPastRuns(all_five_days_elapsed_time)


if __name__ == "__main__":
    main()
