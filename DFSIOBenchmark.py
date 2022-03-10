import time
import paramiko
import re
from matplotlib import pyplot as plt
import numpy as np
from bs4 import BeautifulSoup
import requests


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


# Handle the FS, chose between ozone and HDFS
def fsHandle():  # done
    current_fs = re.search('<value>(.*)</value>', getCurrentFS().strip()).group(1)
    print("Current FS is: ", current_fs)
    str_ozone = "ofs://ozone1"
    str_ozone = str_ozone.replace("/", "\/")
    str_HDFS = "hdfs://" + getHDFSNameNode() + ":8020"
    str_HDFS = str_HDFS.replace("/", "\/")
    current_fs = current_fs.replace("/", "\/")
    if_switch = input("Do you want to switch the FS?\n")

    if if_switch.lower() == "yes":
        new_FS = str_ozone if current_fs == str_HDFS else str_HDFS
    else:
        new_FS = current_fs
    print(new_FS)
    changeCoreSite(current_fs, new_FS)
    return new_FS, str_ozone


# Run Teragen
# def teraGen(user, jar_loc):  # done
#     command_formation = f"sudo -u {user} /opt/cloudera/parcels/CDH/bin/hadoop jar {jar_loc} teragen 1000 " \
#                         f"/tera/teraoutputs/terasort-input "
#     stdin, stdout, stderr = c.exec_command(command_formation)
#     time.sleep(5)
#     return stdout, stderr


# def createFile(file_name):
#     c.exec_command(f"touch /tmp/{file_name}")


def DFSIO(nr_files, file_size, job_type, user, jar_loc, file_name):
    command_formation = f"sudo -u {user} -s /opt/cloudera/parcels/CDH/bin/hadoop jar {jar_loc} TestDFSIO -Dtest.build.data=/testing/benchmarks/outputs -{job_type} -nrFiles {nr_files} -fileSize {file_size} -resFile /tmp/{file_name}"
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
def createDirectory():  # done
    command_formation = f"sudo -u pratyush ozone fs -mkdir -p ofs://ozone1/testing/benchmarks/"
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
def scrapeData(output, user, jar_loc, file_name):  # done
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
        main_exec(user, file_name, jar_loc)
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
    for data in dfsio_array:
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

    return y_arr


# Plot the graph from the scrapped data
def plotGraph(write_result, read_result):  # done
    properties = ['Elapsed:', 'Average Map Time', 'Average Shuffle Time', 'Average Merge Time', 'Average Reduce Time']
    dfsio_array = [write_result, read_result]

    write_y, read_y = getTimeTaken(dfsio_array, properties)

    barWidth = 0.25
    fig = plt.subplots(figsize=(12, 8))
    br1 = np.arange(len(write_y))
    br2 = [x + barWidth for x in br1]

    plt.bar(br1, write_y, color='r', width=barWidth,
            edgecolor='grey', label='DFSIO write')
    plt.bar(br2, read_y, color='g', width=barWidth,
            edgecolor='grey', label='DFSIO read')

    plt.xlabel('Metric', fontweight='bold', fontsize=15, labelpad=15)
    plt.ylabel('Time taken', fontweight='bold', fontsize=15, labelpad=15)
    plt.xticks([r + barWidth for r in range(len(write_y))],
               properties)

    plt.legend()
    plt.grid(color='#95a5a6', linestyle='solid', linewidth=1, axis='y', alpha=0.2)
    plt.grid(color='#95a5a6', linestyle='solid', linewidth=1, axis='x', alpha=0.2)
    plt.show()


def main_exec(user, file_name, jar_loc):
    print("DFSIO write executing....")
    stderr, stdout = DFSIO('2', '16KB', 'write', user, jar_loc, file_name)
    err_relaxed = stderr.read().decode().strip()
    print(err_relaxed)
    out = stdout.read().decode().strip()
    print(out)
    write_result = scrapeData(out, user, jar_loc, file_name)

    print("DFSIO read executing....")
    stderr1, stdout1 = DFSIO('2', '16KB', 'read', user, jar_loc, file_name)
    err_relaxed1 = stderr1.read().decode().strip()
    print(err_relaxed1)
    out1 = stdout1.read().decode().strip()
    # print(out1)
    read_result = scrapeData(out1, user, jar_loc, file_name)

    # Printing all logs
    print(out)
    print(out1)

    # Print the results
    print("DFSIO write result", write_result)
    print("DFSIO read result", read_result)

    # Plot graphs
    plotGraph(write_result, read_result)


# Closing the connection
def closeConnection():  # done
    c.close()
    print("SSH Closed....")
    exit(0)


c = sshConnect()


def main():
    new_fs, str_ozone = fsHandle()  # Handle between ozone and HDFS
    if new_fs == str_ozone:
        # Clean the directory
        removeDirectory("sudo -u pratyush ozone fs -rm -r -skipTrash ofs://ozone1/testing/")
        print("Directory removed")
        # Create the directory
        createDirectory()
        print("Directory created")
        user = "pratyush"
    else:
        removeDirectory("sudo -u hdfs -s hdfs dfs -rm -r -skipTrash /testing")
        user = "hdfs"

    jar_loc = getJarLocation().strip()
    file_name = input("Enter output file name : ")
    # createFile(file_name)

    main_exec(user, file_name, jar_loc)

    # Close the connection
    closeConnection()


if __name__ == "__main__":
    main()
