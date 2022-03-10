import urllib
from pymongo import MongoClient
from matplotlib import pyplot as plt
import re
import numpy as np

# url = "mongodb+srv://" + pratyushbhatt1617:Pratyush@123@cluster0.ckxbw.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
username = "pratyushbhatt1617"
password = "Pratyush@123"
url = "mongodb+srv://" + urllib.parse.quote_plus("pratyushbhatt1617") + ":" + urllib.parse.quote_plus("Pratyush@123") + \
      "@cluster0.ckxbw.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"

cluster = MongoClient(url)
db = cluster["benchmark"]
collection = db["Terasuite"]

dict_array = collection.find().sort("_id", 1).limit(5)
last_five_days_Data = []
for key in dict_array:
    lists = [key['teragen'], key['terasort'], key['teravalidate']]
    last_five_days_Data.append(lists)

print(last_five_days_Data[0])
# print(x)

#
y_arr = []
#
properties = ['Elapsed:', 'Average Map Time', 'Average Shuffle Time', 'Average Merge Time', 'Average Reduce Time']
#
for data in last_five_days_Data[0]:
    # print(type(data))
    y = []
    for property in properties:
        if property in data:
            temp = re.findall(r'\d+', data[property])
            # print(temp)
            result = 0
            for i, value in reversed(list(enumerate(temp))):
                result += (int(value) * (60 ** (len(temp) - i - 1)))
            #     print(i,value)
            y.append(result)
        else:
            y.append(0)

    y_arr.append(y)

teragen_y, terasort_y, teravalidate_y = y_arr
teragen_y2, terasort_y2, teravalidate_y2 = y_arr

print(teragen_y, terasort_y, teravalidate_y)

barWidth = 0.25
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(25, 10))
br1 = np.arange(len(teragen_y))
br2 = [x + barWidth for x in br1]
br3 = [x + barWidth for x in br2]

ax1.bar(br1, teragen_y, color='r', width=barWidth,
        edgecolor='grey', label='Teragen')
ax1.bar(br2, terasort_y, color='g', width=barWidth,
        edgecolor='grey', label='Terasort')
ax1.bar(br3, teravalidate_y, color='b', width=barWidth,
        edgecolor='grey', label='Teravalidate')

ax2.bar(br1, teragen_y2, color='r', width=barWidth,
        edgecolor='grey', label='Teragen')
ax2.bar(br2, terasort_y2, color='g', width=barWidth,
        edgecolor='grey', label='Terasort')
ax2.bar(br3, teravalidate_y2, color='b', width=barWidth,
        edgecolor='grey', label='Teravalidate')

ax1.set_xlabel('Metric', fontweight='bold', fontsize=15, labelpad=10)
ax1.set_ylabel('Time taken', fontweight='bold', fontsize=15, labelpad=10)
ax1.set_xticks([r + barWidth for r in range(len(teragen_y))],
               properties, rotation=-8)

ax2.set_xlabel('Metric', fontweight='bold', fontsize=15, labelpad=10)
ax2.set_ylabel('Time taken', fontweight='bold', fontsize=15, labelpad=10)
ax2.set_xticks([r + barWidth for r in range(len(teragen_y2))],
               properties, rotation=-8)

ax1.legend()
ax2.legend()


ax1.grid(color='#95a5a6', linestyle='solid', linewidth=1, axis='y', alpha=0.2)
ax1.grid(color='#95a5a6', linestyle='solid', linewidth=1, axis='x', alpha=0.2)

ax2.grid(color='#95a5a6', linestyle='solid', linewidth=1, axis='y', alpha=0.2)
ax2.grid(color='#95a5a6', linestyle='solid', linewidth=1, axis='x', alpha=0.2)
# plt.tight_layout()
plt.show()

# post = {"_id":0 , "name":"pratyush","score":5}
# collection.insert_one(post)


# post1 = {"_id":1 , "name":"Anuj","score":15}
# post2 = {"_id":2 , "name":"Puneet","score":52}
# collection.insert_many([post1,post2])

# posts = [{"name":"anushka","score":1},{"name":"garima","score":2},{"name":"shivangi","score":3},{"name":"deepali","score":4},{"name":"raksha","score":5}]
#
# collection.insert_many(posts)

# lists = collection.find().sort("_id", 1)
# print(lists)
#
# for x in lists:
#     print(x)
# collection.delete_many({})


# mongodb+srv://pratyushbhatt1617:<password>@cluster0.ckxbw.mongodb.net/myFirstDatabase?retryWrites=true&w=majority
