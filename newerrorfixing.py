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


def DFSIO():
    command_formation = f"sudo -u hdfs -s /opt/cloudera/parcels/CDH/bin/hadoop jar /opt/cloudera/parcels/CDH/jars/hadoop-mapreduce-client-jobclient-3.1.1.7.1.8.0-515-tests.jar TestDFSIO -Dtest.build.data=/testing/benchmarks/outputs -write -nrFiles 2 -fileSize 16MB -resFile /tmp/pratyushdfsionewnew.txt"
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


# Closing the connection
def closeConnection():  # done
    c.close()
    print("SSH Closed....")
    exit(0)


def createFile(file_name):
    c.exec_command(f"touch /tmp/{file_name}")


c = sshConnect()


def main():
    # print("DFSIO write executing....")
    # stderr, stdout = DFSIO()
    # err_relaxed = stderr.read().decode().strip()
    # print(err_relaxed)
    # out = stdout.read().decode().strip()
    # print(out)
    file_name = input("Enter the filename: ")
    createFile(file_name)
    # Close the connection
    closeConnection()


if __name__ == "__main__":
    main()
