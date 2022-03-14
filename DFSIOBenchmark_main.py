from matplotlib import pyplot as plt
from pymongo import MongoClient
from bs4 import BeautifulSoup
import time
import paramiko
import re
import numpy as np
import requests
import urllib

FILE_NAME = "myfile"
N_FILES = '32'
FILE_SIZE = '1GB'


# SSH Connection to Ozone
def sshConnect():  # done
    private_key = paramiko.RSAKey.from_private_key_file("myKeys", password="company")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print("connecting....")
    client.connect(hostname="172.27.68.134", username="root", pkey=private_key, password="password")
    print("connected....")
    return client


# Get the HDFS nameNode
def getHDFSNameNode():  # done
    command_formation = "hdfs getconf -namenodes"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    out = stdout.read().decode().strip()
    return out


# Get the current FS
def getCurrentFS():  # done
    command_formation = "grep  -m 1 '<value>' /etc/hadoop/conf/core-site.xml"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    out = stdout.read().decode().strip()
    return out


# Change the core-site.xml 's fs.defaultFS value as according to ozone or HDFS chosen by the user
def changeCoreSite(current_fs, new_fs):  # done
    command_formation = f"grep -i -m 1 '<value>' /etc/hadoop/conf/core-site.xml | sed -i '0,/{current_fs}/s//{new_fs}/' /etc/hadoop/conf/core-site.xml"
    c.exec_command(command_formation)
    time.sleep(5)


def getCdhVersion():
    command_formation = f"ls -l /opt/cloudera/parcels"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    result = stdout.read().decode().strip()
    lists = result.split("\n")[2].split(" ")
    required_string = lists[len(lists) - 1].split("-")[1]
    return required_string


def run(str_ozone, current_fs, jar_loc):
    print("\nCurrent FS is : ", current_fs.replace("\/", "/"))
    if current_fs == str_ozone:
        user = "systest"
        # Clean the directory
        removeDirectory(f"sudo -u {user} ozone fs -rm -r -skipTrash ofs://ozone1/testing/")
        print("Directory cleaned")
        # Create the directory
        createDirectory(user)
        print("Directory created")

    else:
        user = "hdfs"
        print("Directory cleaned")
        removeDirectory(f"sudo -u {user} -s hdfs dfs -rm -r -skipTrash /testing")
    FS = "HDFS" if "hdfs" in current_fs else "OZONE"  # Getting the current FS name
    data_to_plot = main_exec(user, jar_loc, FS)
    return FS, data_to_plot


# Handle the FS, chose between ozone and HDFS
def fsHandle(current_fs, str_HDFS, jar_loc):  # done
    # print("Current FS is: ", current_fs)
    str_ozone = "ofs://ozone1"
    str_ozone = str_ozone.replace("/", "\/")
    str_HDFS = str_HDFS.replace("/", "\/")
    current_fs = current_fs.replace("/", "\/")
    # if_switch = input("Do you want to switch the FS?\n")

    data_1_FS, data_to_plot_1 = run(str_ozone, current_fs, jar_loc)

    old_fs = current_fs
    current_fs = str_ozone if current_fs == str_HDFS else str_HDFS
    changeCoreSite(old_fs, current_fs)

    data_2_FS, data_to_plot_2 = run(str_ozone, current_fs, jar_loc)

    return data_to_plot_1, data_1_FS, data_to_plot_2, data_2_FS


# Run DFSIO
def DFSIO(job_type, user, jar_loc, file_name):
    command_formation = f"sudo -u {user} -s /opt/cloudera/parcels/CDH/bin/hadoop jar {jar_loc} TestDFSIO -Dtest.build.data=/testing/benchmarks/outputs -{job_type} -nrFiles {N_FILES} -fileSize {FILE_SIZE} -resFile /tmp/{file_name}"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    return stdout, stderr


# Get the running jar
def getJarLocation():  # done
    command_formation = f"find /opt/cloudera/parcels/CDH/jars -name '*hadoop*mapreduce*test*'"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    out = stdout.read().decode().strip()
    return out


# Create the directory, only for ozone cases
def createDirectory(user):  # done
    command_formation = f"sudo -u {user} ozone fs -mkdir -p ofs://ozone1/testing/benchmarks/"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    return stdout, stderr


# Remove the directory, to avoid the already present directory errors
def removeDirectory(command):  # done
    command_formation = f"{command}"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    return stdout, stderr


# Scrape the data from the webpage
def scrapeData(output):  # done
    url = re.search(r'The url to track the job: (.*)', output).group(1)
    # Retrieve the HTML
    html_content = requests.get(url).text

    # Make a soup object for easy parsing
    soup = BeautifulSoup(html_content, "html.parser")

    # Get the needed table
    tera_table = soup.find("table", attrs={"class": "info"})

    # Sometimes webpage created are empty, in those cases rerun the code.
    if tera_table is None:
        print("Empty webpage, retrying process again")
        main()
        pass
    else:
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

        return data


# Converting the time into seconds and presentable form
def getTimeTaken(dfsio_array, properties):  # done
    y_arr = []
    # print("Inside getTimeTaken", tera_array)
    for data in dfsio_array:
        y = []
        for p in properties:
            if p in data:
                temp = re.findall(r'\d+', data[p])
                # print(temp)
                result = 0
                for i, value in reversed(list(enumerate(temp))):
                    result += (int(value) * (60 ** (len(temp) - i - 1)))
                #     print(i,value)
                y.append(result)
            else:
                y.append(0)

        y_arr.append(y)

    return y_arr


def removeExtraKeys(write_result, read_result):
    properties = ['Elapsed:', 'Average Map Time', 'Average Shuffle Time', 'Average Merge Time', 'Average Reduce Time']
    to_remove = []
    for key in write_result.keys():
        if key not in properties:
            to_remove.append(key)

    for key in to_remove:
        write_result.pop(key, None)
        read_result.pop(key, None)

    return write_result, read_result


def updateToMongo(doc):
    url = "mongodb+srv://" + urllib.parse.quote_plus("pratyushbhatt1617") + ":" + urllib.parse.quote_plus(
        "Pratyush@123") + \
          "@cluster0.ckxbw.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"

    cluster = MongoClient(url)
    db = cluster["Benchmark2"]
    collection = db[CDH_VERSION]
    collection.insert_one(doc)


def compareResultWithPast(last_five_days):
    pass


def retrieveDataFromMongo(is_tera, FS):
    url = "mongodb+srv://" + urllib.parse.quote_plus("pratyushbhatt1617") + ":" + urllib.parse.quote_plus(
        "Pratyush@123") + \
          "@cluster0.ckxbw.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"

    cluster = MongoClient(url)
    db = cluster["Benchmark2"]
    collection = db[CDH_VERSION]

    dict_array = collection.find().sort("time_stamp", -1).limit(20)
    last_five_days_Data = []
    counter = 0
    for key in dict_array:
        if counter == 5:
            break
        if key['is_tera'] == is_tera and key['file_system'] == FS:
            lists = [key['write'], key['read']]
            last_five_days_Data.append(lists)
            counter += 1

    return last_five_days_Data


# Plot the graph from the scrapped data
def plotGraph(data1, data2, data1_fs, data2_fs):  # done
    properties = ['Elapsed:', 'Average Map Time', 'Average Shuffle Time', 'Average Merge Time', 'Average Reduce Time']
    dfsio_array1 = data1[0]
    dfsio_array2 = data2[0]
    # print("Data before")
    # print(data)
    write_1, read_1 = getTimeTaken(dfsio_array1, properties)
    write_2, read_2 = getTimeTaken(dfsio_array2, properties)
    # print(teragen_y, terasort_y, teravalidate_y)

    barWidth = 0.25
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(25, 10))
    br1 = np.arange(len(write_1))
    br2 = [x + barWidth for x in br1]

    ax1.bar(br1, write_1, color='r', width=barWidth,
            edgecolor='grey', label='write')
    ax1.bar(br2, read_1, color='g', width=barWidth,
            edgecolor='grey', label='read')

    ax2.bar(br1, write_2, color='r', width=barWidth,
            edgecolor='grey', label='write')
    ax2.bar(br2, read_2, color='g', width=barWidth,
            edgecolor='grey', label='read')

    ax1.set_xlabel('Metric', fontweight='bold', fontsize=15, labelpad=10)
    ax1.set_ylabel('Time taken in seconds', fontweight='bold', fontsize=15, labelpad=10)
    ax1.set_xticks([r + barWidth for r in range(len(write_1))],
                   properties, rotation=-8)

    ax2.set_xlabel('Metric', fontweight='bold', fontsize=15, labelpad=10)
    ax2.set_ylabel('Time taken in seconds', fontweight='bold', fontsize=15, labelpad=10)
    ax2.set_xticks([r + barWidth for r in range(len(write_2))],
                   properties, rotation=-8)

    ax1.legend()
    ax2.legend()

    ax1.set_title(data1_fs + f"\n Number of files: {N_FILES}\n File size: {FILE_SIZE}", fontweight="bold")
    ax2.set_title(data2_fs + f"\n Number of files: {N_FILES}\n File size: {FILE_SIZE}", fontweight="bold")

    ax1.grid(color='#95a5a6', linestyle='solid', linewidth=1, axis='y', alpha=0.2)
    ax1.grid(color='#95a5a6', linestyle='solid', linewidth=1, axis='x', alpha=0.2)

    ax2.grid(color='#95a5a6', linestyle='solid', linewidth=1, axis='y', alpha=0.2)
    ax2.grid(color='#95a5a6', linestyle='solid', linewidth=1, axis='x', alpha=0.2)
    # plt.tight_layout()
    plt.show()


def main_exec(user, jar_loc, FS):
    result = FILE_NAME + "_" + user + "_" + str(int(time.time()))
    print("\nDFSIO write executing....")

    stderr, stdout = DFSIO('write', user, jar_loc, result)
    err_relaxed = stderr.read().decode().strip()
    print(err_relaxed)
    out = stdout.read().decode().strip()
    print(out)
    write_result = scrapeData(out)

    print("\nDFSIO read executing....")
    stderr1, stdout1 = DFSIO('read', user, jar_loc, result)
    err_relaxed1 = stderr1.read().decode().strip()
    print(err_relaxed1)
    out1 = stdout1.read().decode().strip()
    # print(out1)
    read_result = scrapeData(out1)

    # Printing all logs
    print(out)
    print(out1)

    write_result, read_result = removeExtraKeys(write_result, read_result)

    # Print the results
    print("Write result", write_result)
    print("Read result", read_result)

    DFSIO_results = {
        "file_system": FS,
        "is_tera": False,
        "time_stamp": int(time.time()),
        "write": write_result,
        "read": read_result,
    }
    updateToMongo(DFSIO_results)
    is_tera = False
    data = retrieveDataFromMongo(is_tera, FS)
    print("retrieving from mongo", data)
    return data


# Closing the connection
def closeConnection():  # done
    c.close()
    print("SSH Closed....")
    exit(0)


c = sshConnect()
CDH_VERSION = getCdhVersion()
jar_location = getJarLocation().strip()


def main():
    str_HDFS = "hdfs://" + getHDFSNameNode() + ":8020"
    current_fs = re.search('<value>(.*)</value>', getCurrentFS().strip()).group(1)
    data1, data1_fs, data2, data2_fs = fsHandle(current_fs, str_HDFS, jar_location)  # Handle between ozone and HDFS
    plotGraph(data1, data2, data1_fs, data2_fs)
    # plotGraph(data2)
    # Close the connection
    closeConnection()


if __name__ == "__main__":
    main()
