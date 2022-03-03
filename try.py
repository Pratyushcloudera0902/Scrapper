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
    client.connect(hostname="172.27.133.3", username="root", pkey=private_key, password="password")
    print("connected!!")
    return client


def switchUser(user):
    command_formation = f"su - {user} ; pwd"
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


def terasort():
    # stdin, stdout, stderr = c.exec_command(command_formation)
    command_formation = f"sudo -u pratyush /opt/cloudera/parcels/CDH/bin/hadoop jar /opt/cloudera/parcels/CDH/jars/hadoop-mapreduce-examples-3.1.1.7.1.8.0-517.jar terasort /teraozonescript/teraoutput/terasort-input /teraozonescript/teraoutput/terasort-output"
    # command_formation = f"sudo -u pratyush ozone fs -mkdir -p ofs://ozone1/teraozonescript/teraoutput"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    return stdout, stderr
    # return out


def getJarLocation():
    command_formation = f"find /opt/cloudera/parcels/CDH/jars -name '*hadoop*mapreduce*example*'"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    out = stdout.read().decode().strip()
    return out


def teragen():
    # stdin, stdout, stderr = c.exec_command(command_formation)
    command_formation = f"sudo -u pratyush /opt/cloudera/parcels/CDH/bin/hadoop jar /opt/cloudera/parcels/CDH/jars/hadoop-mapreduce-examples-3.1.1.7.1.8.0-517.jar teragen 100000000 /teraozonescript/teraoutput/terasort-input"
    # command_formation = f"sudo -u pratyush ozone fs -mkdir -p ofs://ozone1/teraozonescript/teraoutput"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    return stdout, stderr
    # return out


def getUser():
    command_formation = f"whoami"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    # inn = stdin.read().decode().strip()
    out = stdout.read().decode().strip()
    print(out)


c = sshConnect()


def main():
    channel = c.get_transport().open_session()
    channel.invoke_shell()

    # while channel.recv_ready():
    #     channel.recv(1024)

    channel.send("su - pratyush")
    dragon = str()
    while channel.recv_ready():
        dragon += channel.recv(1024)

    # print(dragon.decode().strip())
    channel.sendall("cd /tmp/\n")

    channel.sendall("pwd\n")
    print(channel.recv(1024))
    channel.sendall("mkdir drago6\n")
    channel.sendall("ls\n")
    print(channel.recv(9999).decode().strip())


if __name__ == "__main__":
    main()

# c.close()
# print("SSH Closed!")
