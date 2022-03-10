from matplotlib import pyplot as plt
from pymongo import MongoClient
from bs4 import BeautifulSoup
import time
import paramiko
import re
import numpy as np
import requests
import urllib


# SSH Connection to Ozone
def sshConnect():  # done
    private_key = paramiko.RSAKey.from_private_key_file("myKeys", password="company")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print("connecting....")
    client.connect(hostname="172.27.38.132", username="root", pkey=private_key, password="password")
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


def run(str_ozone, current_fs, str_HDFS, jar_loc):
    new_FS = str_ozone if current_fs == str_HDFS else str_HDFS
    print(new_FS)
    changeCoreSite(current_fs, new_FS)
    if new_FS == str_ozone:
        # Clean the directory
        removeDirectory("sudo -u pratyush ozone fs -rm -r -skipTrash ofs://ozone1/tera/")
        print("Directory removed")
        # Create the directory
        createDirectory()
        print("Directory created")
        user = "pratyush"
    else:
        removeDirectory("sudo -u hdfs hdfs dfs -rm -r -skipTrash /tera")
        user = "hdfs"

    data_to_plot = main_exec(user, jar_loc)
    return new_FS, data_to_plot


# Handle the FS, chose between ozone and HDFS
def fsHandle(current_fs, str_HDFS, jar_loc):  # done
    print("Current FS is: ", current_fs)
    str_ozone = "ofs://ozone1"
    str_ozone = str_ozone.replace("/", "\/")
    str_HDFS = str_HDFS.replace("/", "\/")
    current_fs = current_fs.replace("/", "\/")
    # if_switch = input("Do you want to switch the FS?\n")

    current_fs, data_to_plot_1 = run(str_ozone, current_fs, str_HDFS, jar_loc)
    data_1_FS = "HDFS" if "hdfs" in current_fs else "OZONE"
    current_fs, data_to_plot_2 = run(str_ozone, current_fs, str_HDFS, jar_loc)
    data_2_FS = "HDFS" if "hdfs" in current_fs else "OZONE"
    return data_to_plot_1, data_1_FS, data_to_plot_2, data_2_FS


# Run Teragen
def teraGen(user, jar_loc):  # done
    command_formation = f"sudo -u {user} -s /opt/cloudera/parcels/CDH/bin/hadoop jar {jar_loc} teragen 1000 " \
                        f"/tera/teraoutputs/terasort-input "
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    return stdout, stderr


# Run Terasort
def teraSort(user, jar_loc):  # done
    command_formation = f"sudo -u {user} -s /opt/cloudera/parcels/CDH/bin/hadoop jar {jar_loc} terasort " \
                        f"/tera/teraoutputs/terasort-input /tera/teraoutputs/terasort-output "
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    return stdout, stderr


# Run Teravalidate
def teraValidate(user, jar_loc):  # done
    command_formation = f"sudo -u {user} -s /opt/cloudera/parcels/CDH/bin/hadoop jar {jar_loc} teravalidate " \
                        f"/tera/teraoutputs/terasort-output /tera/teraoutputs/teravalidate-output "
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    return stdout, stderr


# Get the running jar
def getJarLocation():  # done
    command_formation = f"find /opt/cloudera/parcels/CDH/jars -name '*hadoop*mapreduce*example*'"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    out = stdout.read().decode().strip()
    return out


# Create the directory, only for ozone cases
def createDirectory():  # done
    command_formation = f"sudo -u pratyush ozone fs -mkdir -p ofs://ozone1/tera/teraoutputs/"
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
def scrapeData(output, user, jar_loc):  # done
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
        main_exec(user, jar_loc)
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
def getTimeTaken(tera_array, properties):  # done
    y_arr = []
    # print("Inside getTimeTaken", tera_array)
    for data in tera_array:
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


def removeExtraKeys(teragen_result, terasort_result, teravalidate_result):
    properties = ['Elapsed:', 'Average Map Time', 'Average Shuffle Time', 'Average Merge Time', 'Average Reduce Time']
    to_remove = []
    for key in terasort_result.keys():
        if key not in properties:
            to_remove.append(key)

    for key in to_remove:
        teragen_result.pop(key, None)
        terasort_result.pop(key, None)
        teravalidate_result.pop(key, None)

    return teragen_result, terasort_result, teravalidate_result


def updateToMongo(docs):
    url = "mongodb+srv://" + urllib.parse.quote_plus("pratyushbhatt1617") + ":" + urllib.parse.quote_plus(
        "Pratyush@123") + \
          "@cluster0.ckxbw.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"

    cluster = MongoClient(url)
    db = cluster["benchmark"]
    collection = db["Terasuite"]
    collection.insert_one(docs)


def retrieveDataFromMongo():
    url = "mongodb+srv://" + urllib.parse.quote_plus("pratyushbhatt1617") + ":" + urllib.parse.quote_plus(
        "Pratyush@123") + \
          "@cluster0.ckxbw.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"

    cluster = MongoClient(url)
    db = cluster["benchmark"]
    collection = db["Terasuite"]

    dict_array = collection.find().sort("_id", -1).limit(5)
    last_five_days_Data = []
    for key in dict_array:
        lists = [key['teragen'], key['terasort'], key['teravalidate']]
        last_five_days_Data.append(lists)

    return last_five_days_Data


# Plot the graph from the scrapped data
def plotGraph(data1, data2, data1_fs, data2_fs):  # done
    properties = ['Elapsed:', 'Average Map Time', 'Average Shuffle Time', 'Average Merge Time', 'Average Reduce Time']
    tera_array1 = data1[0]
    tera_array2 = data2[0]
    # print("Data before")
    # print(data)
    teragen_y1, terasort_y1, teravalidate_y1 = getTimeTaken(tera_array1, properties)
    teragen_y2, terasort_y2, teravalidate_y2 = getTimeTaken(tera_array2, properties)
    # print(teragen_y, terasort_y, teravalidate_y)

    barWidth = 0.25
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(25, 10))
    br1 = np.arange(len(teragen_y1))
    br2 = [x + barWidth for x in br1]
    br3 = [x + barWidth for x in br2]

    ax1.bar(br1, teragen_y1, color='r', width=barWidth,
            edgecolor='grey', label='Teragen')
    ax1.bar(br2, terasort_y1, color='g', width=barWidth,
            edgecolor='grey', label='Terasort')
    ax1.bar(br3, teravalidate_y1, color='b', width=barWidth,
            edgecolor='grey', label='Teravalidate')

    ax2.bar(br1, teragen_y2, color='r', width=barWidth,
            edgecolor='grey', label='Teragen')
    ax2.bar(br2, terasort_y2, color='g', width=barWidth,
            edgecolor='grey', label='Terasort')
    ax2.bar(br3, teravalidate_y2, color='b', width=barWidth,
            edgecolor='grey', label='Teravalidate')

    ax1.set_xlabel('Metric', fontweight='bold', fontsize=15, labelpad=10)
    ax1.set_ylabel('Time taken', fontweight='bold', fontsize=15, labelpad=10)
    ax1.set_xticks([r + barWidth for r in range(len(teragen_y1))],
                   properties, rotation=-8)

    ax2.set_xlabel('Metric', fontweight='bold', fontsize=15, labelpad=10)
    ax2.set_ylabel('Time taken', fontweight='bold', fontsize=15, labelpad=10)
    ax2.set_xticks([r + barWidth for r in range(len(teragen_y2))],
                   properties, rotation=-8)

    ax1.legend()
    ax2.legend()

    ax1.set_title(data1_fs, fontweight="bold")
    ax2.set_title(data2_fs, fontweight="bold")

    ax1.grid(color='#95a5a6', linestyle='solid', linewidth=1, axis='y', alpha=0.2)
    ax1.grid(color='#95a5a6', linestyle='solid', linewidth=1, axis='x', alpha=0.2)

    ax2.grid(color='#95a5a6', linestyle='solid', linewidth=1, axis='y', alpha=0.2)
    ax2.grid(color='#95a5a6', linestyle='solid', linewidth=1, axis='x', alpha=0.2)
    # plt.tight_layout()
    plt.show()


def main_exec(user, jar_loc):
    print("Teragen executing....")
    stderr, stdout = teraGen(user, jar_loc)
    err_relaxed = stderr.read().decode().strip()
    print(err_relaxed)
    out = stdout.read().decode().strip()
    # print(out)
    teragen_result = scrapeData(out, user, jar_loc)

    print("Terasort executing....")
    stderr1, stdout1 = teraSort(user, jar_loc)
    err_relaxed1 = stderr1.read().decode().strip()
    print(err_relaxed1)
    out1 = stdout1.read().decode().strip()
    # print(out1)
    terasort_result = scrapeData(out1, user, jar_loc)

    print("Teravalidate executing....")
    stderr2, stdout2 = teraValidate(user, jar_loc)
    err_relaxed2 = stderr2.read().decode().strip()
    print(err_relaxed2)
    out2 = stdout2.read().decode().strip()
    # print(out2)
    teravalidate_result = scrapeData(out2, user, jar_loc)

    # Printing all logs
    print(out)
    print(out1)
    print(out2)

    teragen_result, terasort_result, teravalidate_result = removeExtraKeys(teragen_result, terasort_result,
                                                                           teravalidate_result)

    # Print the results
    print("Teragen result", teragen_result)
    print("Terasort result", terasort_result)
    print("Teravalidate result", teravalidate_result)
    tera_results = {
        "teragen": teragen_result,
        "terasort": terasort_result,
        "teravalidate": teravalidate_result
    }
    updateToMongo(tera_results)
    data = retrieveDataFromMongo()
    print("retrieving from mongo", data)
    return data


# Closing the connection
def closeConnection():  # done
    c.close()
    print("SSH Closed....")
    exit(0)


c = sshConnect()


def main():
    jar_loc = getJarLocation().strip()
    current_fs = re.search('<value>(.*)</value>', getCurrentFS().strip()).group(1)
    str_HDFS = "hdfs://" + getHDFSNameNode() + ":8020"

    data1, data1_fs, data2, data2_fs = fsHandle(current_fs, str_HDFS, jar_loc)  # Handle between ozone and HDFS
    plotGraph(data1, data2, data1_fs, data2_fs)
    # plotGraph(data2)
    # Close the connection
    closeConnection()


if __name__ == "__main__":
    main()
