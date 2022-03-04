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
    print("connecting...")
    client.connect(hostname="172.27.133.3", username="root", pkey=private_key, password="password")
    print("connected....")
    return client


def getHDFSNameNode():
    command_formation = "hdfs getconf -namenodes"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    out = stdout.read().decode().strip()
    return out


def getCurrentFS():
    command_formation = "grep  -m 1 '<value>' /etc/hadoop/conf/core-site.xml"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    out = stdout.read().decode().strip()
    return out


def changeCoreSite(current_fs, new_fs):
    command_formation = f"grep -i -m 1 '<value>' /etc/hadoop/conf/core-site.xml | sed -i '0,/{current_fs}/s//{new_fs}/' /etc/hadoop/conf/core-site.xml"
    c.exec_command(command_formation)
    time.sleep(5)


def fsHandle():
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


def teraGen(user, jar_loc):
    command_formation = f"sudo -u {user} /opt/cloudera/parcels/CDH/bin/hadoop jar {jar_loc} teragen 1000 " \
                        f"/tera/teraoutputs/terasort-input "
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    return stdout, stderr


def teraSort(user, jar_loc):
    command_formation = f"sudo -u {user} /opt/cloudera/parcels/CDH/bin/hadoop jar {jar_loc} terasort " \
                        f"/tera/teraoutputs/terasort-input /tera/teraoutputs/terasort-output "
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    return stdout, stderr


def teraValidate(user, jar_loc):
    command_formation = f"sudo -u {user} /opt/cloudera/parcels/CDH/bin/hadoop jar {jar_loc} teravalidate " \
                        f"/tera/teraoutputs/terasort-output /tera/teraoutputs/teravalidate-output "
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    return stdout, stderr


def getJarLocation():
    command_formation = f"find /opt/cloudera/parcels/CDH/jars -name '*hadoop*mapreduce*example*'"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    out = stdout.read().decode().strip()
    return out


def createDirectory():
    command_formation = f"sudo -u pratyush ozone fs -mkdir -p ofs://ozone1/tera/teraoutputs/"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    return stdout, stderr


def removeDirectory(command):
    command_formation = f"{command}"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    return stdout, stderr


def scrapeData(output):
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


def getTimeTaken(tera_array, properties):
    y_arr = []
    for data in tera_array:
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


def plotGraph(teragen_result, terasort_result, teravalidate_result):
    properties = ['Elapsed:', 'Average Map Time', 'Average Shuffle Time', 'Average Merge Time', 'Average Reduce Time']
    tera_array = [teragen_result, terasort_result, teravalidate_result]

    teragen_y, terasort_y, teravalidate_y = getTimeTaken(tera_array, properties)

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

    plt.xlabel('Metric', fontweight='bold', fontsize=15, labelpad=15)
    plt.ylabel('Time taken', fontweight='bold', fontsize=15, labelpad=15)
    plt.xticks([r + barWidth for r in range(len(teragen_y))],
               properties)

    plt.legend()
    plt.grid(color='#95a5a6', linestyle='solid', linewidth=1, axis='y', alpha=0.2)
    plt.grid(color='#95a5a6', linestyle='solid', linewidth=1, axis='x', alpha=0.2)
    plt.show()


def closeConnection():
    c.close()
    print("SSH Closed....")
    exit(0)


c = sshConnect()


def main():
    new_fs, str_ozone = fsHandle()
    if new_fs == str_ozone:
        # Clean the directory
        removeDirectory("sudo -u pratyush ozone fs -rm -r -skipTrash ofs://ozone1/tera/")
        print("Directory removed")
        # Create the directory
        createDirectory()
        print("Directory created")
        user = "pratyush"
    else:
        removeDirectory("hdfs dfs -rm -r -skipTrash /tera")
        user = "hdfs"

    jar_loc = getJarLocation().strip()

    print("Teragen executing....")
    stderr, stdout = teraGen(user, jar_loc)
    err_relaxed = stderr.read().decode().strip()
    print(err_relaxed)
    out = stdout.read().decode().strip()
    # print(out)
    teragen_result = scrapeData(out)

    print("Terasort executing....")
    stderr1, stdout1 = teraSort(user, jar_loc)
    err_relaxed1 = stderr1.read().decode().strip()
    print(err_relaxed1)
    out1 = stdout1.read().decode().strip()
    # print(out1)
    terasort_result = scrapeData(out1)

    print("Teravalidate executing....")
    stderr2, stdout2 = teraValidate(user, jar_loc)
    err_relaxed2 = stderr2.read().decode().strip()
    print(err_relaxed2)
    out2 = stdout2.read().decode().strip()
    # print(out2)
    teravalidate_result = scrapeData(out2)

    # Printing all logs
    print(out)
    print(out1)
    print(out2)

    # Print the results
    print("Teragen result", teragen_result)
    print("Terasort result", terasort_result)
    print("Teravalidate result", teravalidate_result)

    plotGraph(teragen_result, terasort_result, teravalidate_result)

    # Plot graphs

    # Close the connection
    closeConnection()


if __name__ == "__main__":
    main()
