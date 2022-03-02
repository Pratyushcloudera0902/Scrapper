import json
import time
import paramiko


# import re

# SSH Connection to Ozone
def sshConnect():  # done
    private_key = paramiko.RSAKey.from_private_key_file("myKeys", password="company")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print("connecting...")
    client.connect(hostname="172.27.38.132", username="root", pkey=private_key, password="password")
    print("connected!!")
    return client


def switchUser(user):
    command_formation = f"su - {user}"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    # err = stderr.read().decode().strip()
    # print(type(err))
    # print(err)
    return stderr, stdout


def createLocationOzone(vol_name, buck_name):
    command_formation = f"ozone fs -mkdir -p ofs://ozone1/{vol_name}/{buck_name}"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    out = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    print(out, err)
    print(type(out), type(err))
    return err


def teragen(size, output_folder):
    pass
    # stdin, stdout, stderr = c.exec_command(command_formation)


def getJarLocation():
    command_formation = f"find /opt/cloudera/parcels/CDH/jars -name '*hadoop*mapreduce*example*'"
    stdin, stdout, stderr = c.exec_command(command_formation)
    out = stdout.read().decode().strip()
    return out


def teragen(jar_location, output_location, size):
    command_formation = f"hadoop jar {jar_location} teragen {size} {output_location}"
    stdin, stdout, stderr = c.exec_command(command_formation)
    out = stdout.read().decode().strip()
    return out


def getUser():
    command_formation = f"pwd"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    # inn = stdin.read().decode().strip()
    out = stdout.read().decode().strip()
    print(out)


c = sshConnect()


def main():
    # switchUser("pratyush")
    getUser()
    err, out = switchUser("pratyush")
    err_relaxed = err.read().decode().strip()
    out_relaxed = out.read().decode().strip()
    print(err_relaxed, out_relaxed)
    getUser()
    # err = createLocationOzone("scrappe5", "scrapbucket")
    # if err == "":
    #     print("Location created")
    # else:
    #     print("Failed!", err)
    # location = getJarLocation()
    # print(location)
    #
    # results = teragen(location, "/tmp/terascript/teraoutputs/terasort-input", "100000000")
    # print(results)
    # terasort()
    # teraValidate()


if __name__ == "__main__":
    main()

# c.close()
# print("SSH Closed!")
