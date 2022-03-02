from bs4 import BeautifulSoup
import requests
import csv

# Retrieve the HTML
url = "http://pratyush-1.pratyush.root.hwx.site:19888/jobhistory/job/job_1646194582429_0006"
html_content = requests.get(url).text

# Make a soup object for easy parsing
soup = BeautifulSoup(html_content, "html.parser")

# Get the needed table
tera_table = soup.find("table", attrs={"class": "info"})

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

# write the result into a CSV
a_file = open("results.csv", "w")

writer = csv.writer(a_file)
for key, value in data.items():
    writer.writerow([key, value])

a_file.close()
