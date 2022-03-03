from bs4 import BeautifulSoup
import requests
from matplotlib import pyplot as plt
from matplotlib import pylab
import re

# Retrieve the HTML
url = "http://pratyush-1.pratyush.root.hwx.site:19888/jobhistory/job/job_1646194582429_00024"
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

x = []
y = []

properties = ['Elapsed:', 'Average Map Time', 'Average Shuffle Time', 'Average Merge Time', 'Average Reduce Time']

for property in properties:
    x.append(property)
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

# print(x)
# print(y)

# naming of x-axis and y-axis
fig = plt.figure(figsize=(12, 8))
# naming the title of the plot

plt.bar(x,y)
plt.xlabel("Metrics",fontsize=12,labelpad=12)
plt.ylabel("Time taken",fontsize=12,labelpad=12)
plt.show()

