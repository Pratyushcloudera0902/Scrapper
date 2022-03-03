import time
import paramiko
import re
from bs4 import BeautifulSoup
import requests


# SSH Connection to Ozone
def sshConnect():  # done
    private_key = paramiko.RSAKey.from_private_key_file("myKeys", password="company")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print("connecting...")
    client.connect(hostname="172.27.133.3", username="root", pkey=private_key, password="password")
    print("connected!!")
    return client


def teraGen(jar_loc):
    command_formation = f"sudo -u pratyush /opt/cloudera/parcels/CDH/bin/hadoop jar {jar_loc} teragen 1000 /tera/teraoutputs/terasort-input"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    return stdout, stderr


def teraSort(jar_loc):
    command_formation = f"sudo -u pratyush /opt/cloudera/parcels/CDH/bin/hadoop jar {jar_loc} terasort /tera/teraoutputs/terasort-input /tera/teraoutputs/terasort-output"
    stdin, stdout, stderr = c.exec_command(command_formation)
    time.sleep(5)
    return stdout, stderr


def teraValidate(jar_loc):
    command_formation = f"sudo -u pratyush /opt/cloudera/parcels/CDH/bin/hadoop jar {jar_loc} teravalidate /tera/teraoutputs/terasort-output /tera/teraoutputs/teravalidate-output"
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


def removeDirectory():
    command_formation = f"sudo -u pratyush ozone fs -rm -r -skipTrash ofs://ozone1/tera/"
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
        exit(0)
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


c = sshConnect()


def main():
    # Clean the directory
    removeDirectory()
    print("Directory removed")
    # Create the directory
    createDirectory()
    print("Directory created")
    jar_loc = getJarLocation().strip()

    print("Teragen executing....")
    stderr, stdout = teraGen(jar_loc)
    err_relaxed = stderr.read().decode().strip()
    print(err_relaxed)
    out = stdout.read().decode().strip()
    # print(out)
    teragen_result = scrapeData(out)

    print("Terasort executing....")
    stderr1, stdout1 = teraSort(jar_loc)
    err_relaxed1 = stderr1.read().decode().strip()
    print(err_relaxed1)
    out1 = stdout1.read().decode().strip()
    # print(out1)
    terasort_result = scrapeData(out1)

    print("Teravalidate executing....")
    stderr2, stdout2 = teraValidate(jar_loc)
    err_relaxed2 = stderr2.read().decode().strip()
    print(err_relaxed2)
    out2 = stdout2.read().decode().strip()
    # print(out2)
    teravalidate_result = scrapeData(out2)

    print("Teragen result", teragen_result)
    print("Terasort result", terasort_result)
    print("Teravalidate result", teravalidate_result)


if __name__ == "__main__":
    main()

# c.close()
# print("SSH Closed!")
