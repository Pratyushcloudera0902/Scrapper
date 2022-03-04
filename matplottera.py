from bs4 import BeautifulSoup
import requests
from matplotlib import pyplot as plt
import re
import numpy as np


# Retrieve the HTML

def returnData(urls):
    dictArray = []
    for url in urls:
        html_content = requests.get(url).text

        # Make a soup object for easy parsing
        soup = BeautifulSoup(html_content, "html.parser")

        # Get the needed table
        tera_table = soup.find("table", attrs={"class": "info"})

        if tera_table is None:
            print("YEs")
            exit(0)
        # Store all the headers

        headers = []
        for i in tera_table.find_all("th"):
            title = i.text.replace('\n', ' ').strip()
            headers.append(title)
        headers = headers[1:]
        # print(headers)

        # Store all the values
        values = []
        for i in tera_table.find_all("td"):
            title = i.text.replace('\n', ' ').strip()
            values.append(title)
        # print(values)

        # Convert into a dictionary
        length = len(headers)
        data = {}
        for i in range(length):
            data[headers[i]] = values[i]
        print(data)
        dictArray.append(data)
    return dictArray


urls = ["http://pra-bup-2.pra-bup.root.hwx.site:8088/proxy/application_1646195852633_0075/",
        "http://pra-bup-2.pra-bup.root.hwx.site:8088/proxy/application_1646195852633_0076/",
        "http://pra-bup-2.pra-bup.root.hwx.site:8088/proxy/application_1646195852633_0077/"]

dict_array = returnData(urls)

print(dict_array)
y_arr = []

properties = ['Elapsed:', 'Average Map Time', 'Average Shuffle Time', 'Average Merge Time', 'Average Reduce Time']

for data in dict_array:
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

print(teragen_y, terasort_y, teravalidate_y)

barWidth = 0.25
fig = plt.subplots(figsize=(12, 8))
br1 = np.arange(len(teragen_y))
br2 = [x + barWidth for x in br1]
br3 = [x + barWidth for x in br2]

plt.bar(br1, teragen_y, color='r', width=barWidth,
        edgecolor='grey', label='Teragen')
plt.bar(br2, terasort_y, color='g', width=barWidth,
        edgecolor='grey', label='Terasort')
plt.bar(br3, teravalidate_y, color='b', width=barWidth,
        edgecolor='grey', label='Teravalidate')

plt.xlabel('Metric', fontweight='bold', fontsize=15,labelpad=15)
plt.ylabel('Time taken', fontweight='bold', fontsize=15,labelpad=15)
plt.xticks([r + barWidth for r in range(len(teragen_y))],
           properties)

plt.legend()
plt.show()
# # print(x)
# # print(y)
#
# # naming of x-axis and y-axis
# fig = plt.figure(figsize=(12, 8))
# # naming the title of the plot
#
# plt.bar(x, y)
# plt.xlabel("Metrics", fontsize=12, labelpad=12)
# plt.ylabel("Time taken", fontsize=12, labelpad=12)
# plt.show()
