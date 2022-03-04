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


c = sshConnect()


def main():
    user_name = input("Enter the user name")
    createUser(user_name)
    closeConnection()


if __name__ == "__main__":
    main()
