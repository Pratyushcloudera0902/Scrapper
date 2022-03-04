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


def getJarLocation():
    command_formation = f"find /opt/cloudera/parcels/CDH/jars -name '*hadoop*mapreduce*example*'"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    out = stdout.read().decode().strip()
    return out


def createUser(user_name):
    c.exec_command(f"useradd {user_name}")
    c.exec_command(f"id {user_name}")
    c.exec_command(f"sudo -u hdfs hdfs dfs -mkdir /user/{user_name}")
    c.exec_command(f"sudo -u hdfs hdfs dfs -chown {user_name}:hdfs /user/{user_name}")
    c.exec_command(f"sudo -u hdfs hdfs dfs -chmod -R 777 /user/{user_name}")


def closeConnection():
    c.close()
    print("SSH Closed....")
    exit(0)


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


c = sshConnect()


def main():
    current_fs = re.search('<value>(.*)</value>', getCurrentFS().strip()).group(1)
    print("Current FS is: ", current_fs)
    str_ozone = "ofs://ozone1"
    str_ozone = str_ozone.replace("/", "\/")
    str_HDFS = "hdfs://" + getHDFSNameNode() + ":8020"
    str_HDFS = str_HDFS.replace("/", "\/")
    current_fs = current_fs.replace("/", "\/")
    if_switch = input("Do you want to switch the FS?")

    if if_switch.lower() == "yes":
        new_FS = str_ozone if current_fs == str_HDFS else str_HDFS
    else:
        new_FS = current_fs
    print(new_FS)
    changeCoreSite(current_fs, new_FS)
    # switch_to = input("Which FS you want to work on? Ozone or HDFS.")
    # if switch_to.lower() == "ozone":
    #     current_fs = str_ozone
    # else:
    #     current_fs = str_HDFS
    # changeCoreSite(current_fs)
    closeConnection()


if __name__ == "__main__":
    main()
