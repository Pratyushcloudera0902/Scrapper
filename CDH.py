# import re
# import time
# import paramiko
#
#
# # SSH Connection to Ozone
# def sshConnect():  # done
#     private_key = paramiko.RSAKey.from_private_key_file("myKeys", password="company")
#     client = paramiko.SSHClient()
#     client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#     print("connecting...")
#     client.connect(hostname="172.27.38.132", username="root", pkey=private_key, password="password")
#     print("connected....")
#     return client
#
#
# def getCDHVersion():
#     command_formation = f"ls -l /opt/cloudera/parcels"
#     stdin, stdout, stderr = c.exec_command(command_formation)
#     time.sleep(5)
#     return stdout.read().decode().strip()
#
#
# def closeConnection():
#     c.close()
#     print("SSH Closed....")
#     exit(0)
#
#
# c = sshConnect()
#
#
# def main():
#     result = getCDHVersion()
#     lists = result.split("\n")[2].split(" ")
#     required_string = lists[len(lists)-1].split(".cdh")[0]
#     print(required_string)
#     # print(len(out))
#     # print(type(out), len(result))
#     closeConnection()
#
#
# if __name__ == "__main__":
#     main()


# import datetime
#
# l = datetime.datetime.utcnow()
# print(l,type(l))


st = "hdfs://pratyush-1.pratyush.root.hwx.site:8020"

st1 = st.replace("/","\/")
print(st.replace("/","\/"))
print(st1)
print(st)




