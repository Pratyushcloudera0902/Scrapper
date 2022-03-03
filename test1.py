import re
from bs4 import BeautifulSoup
import requests
import csv

st = '''WARNING: Use "yarn jar" to launch YARN applications.
22/03/03 06:46:44 INFO client.RMProxy: Connecting to ResourceManager at pra-bup-2.pra-bup.root.hwx.site/172.27.12.195:8032
22/03/03 06:46:45 INFO rpc.RpcClient: Creating Volume: user, with pratyush as owner and space quota set to -1 bytes, counts quota set to -1
22/03/03 06:46:45 INFO rpc.RpcClient: Creating Bucket: user/pratyush, with pratyush as owner and Versioning false and Storage Type set to DISK and Encryption set to false 
22/03/03 06:46:46 WARN impl.MetricsConfig: Cannot locate configuration: tried hadoop-metrics2-xceiverclientmetrics.properties,hadoop-metrics2.properties
22/03/03 06:46:46 INFO impl.MetricsSystemImpl: Scheduled Metric snapshot period at 10 second(s).
22/03/03 06:46:46 INFO impl.MetricsSystemImpl: XceiverClientMetrics metrics system started
22/03/03 06:46:46 INFO metrics.MetricRegistries: Loaded MetricRegistries class org.apache.ratis.metrics.impl.MetricRegistriesImpl
22/03/03 06:46:47 INFO terasort.TeraGen: Generating 100000000 using 2
22/03/03 06:46:47 INFO mapreduce.JobSubmitter: number of splits:2
22/03/03 06:46:47 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1646195852633_0006
22/03/03 06:46:47 INFO mapreduce.JobSubmitter: Executing with tokens: []
22/03/03 06:46:48 INFO conf.Configuration: resource-types.xml not found
22/03/03 06:46:48 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
22/03/03 06:46:48 INFO impl.YarnClientImpl: Submitted application application_1646195852633_0006
22/03/03 06:46:48 INFO mapreduce.Job: The url to track the job: http://pra-bup-2.pra-bup.root.hwx.site:8088/proxy/application_1646195852633_0006/
22/03/03 06:46:48 INFO mapreduce.Job: Running job: job_1646195852633_0006
22/03/03 06:46:58 INFO mapreduce.Job: Job job_1646195852633_0006 running in uber mode : false
22/03/03 06:46:58 INFO mapreduce.Job:  map 0% reduce 0%
22/03/03 06:47:15 INFO mapreduce.Job:  map 15% reduce 0%
22/03/03 06:47:21 INFO mapreduce.Job:  map 23% reduce 0%
22/03/03 06:47:27 INFO mapreduce.Job:  map 31% reduce 0%
22/03/03 06:47:32 INFO mapreduce.Job:  map 35% reduce 0%
22/03/03 06:47:33 INFO mapreduce.Job:  map 39% reduce 0%
22/03/03 06:47:38 INFO mapreduce.Job:  map 43% reduce 0%
22/03/03 06:47:39 INFO mapreduce.Job:  map 47% reduce 0%
22/03/03 06:47:44 INFO mapreduce.Job:  map 52% reduce 0%
22/03/03 06:47:45 INFO mapreduce.Job:  map 56% reduce 0%
22/03/03 06:47:50 INFO mapreduce.Job:  map 58% reduce 0%
22/03/03 06:47:52 INFO mapreduce.Job:  map 62% reduce 0%
22/03/03 06:47:57 INFO mapreduce.Job:  map 66% reduce 0%
22/03/03 06:47:58 INFO mapreduce.Job:  map 71% reduce 0%
22/03/03 06:48:03 INFO mapreduce.Job:  map 75% reduce 0%
22/03/03 06:48:04 INFO mapreduce.Job:  map 79% reduce 0%
22/03/03 06:48:09 INFO mapreduce.Job:  map 84% reduce 0%
22/03/03 06:48:10 INFO mapreduce.Job:  map 88% reduce 0%
22/03/03 06:48:14 INFO mapreduce.Job:  map 91% reduce 0%
22/03/03 06:48:16 INFO mapreduce.Job:  map 95% reduce 0%
22/03/03 06:48:22 INFO mapreduce.Job:  map 97% reduce 0%
22/03/03 06:48:25 INFO mapreduce.Job:  map 100% reduce 0%
22/03/03 06:48:26 INFO mapreduce.Job: Job job_1646195852633_0006 completed successfully
22/03/03 06:48:26 INFO mapreduce.Job: Counters: 33
	File System Counters
		FILE: Number of bytes read=0
		FILE: Number of bytes written=499578
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		OFS: Number of bytes read=170
		OFS: Number of bytes written=10000000000
		OFS: Number of read operations=12
		OFS: Number of large read operations=0
		OFS: Number of write operations=4
	Job Counters 
		Launched map tasks=2
		Other local map tasks=2
		Total time spent by all maps in occupied slots (ms)=157400
		Total time spent by all reduces in occupied slots (ms)=0
		Total time spent by all map tasks (ms)=157400
		Total vcore-milliseconds taken by all map tasks=157400
		Total megabyte-milliseconds taken by all map tasks=161177600
	Map-Reduce Framework
		Map input records=100000000
		Map output records=100000000
		Input split bytes=170
		Spilled Records=0
		Failed Shuffles=0
		Merged Map outputs=0
		GC time elapsed (ms)=927
		CPU time spent (ms)=152170
		Physical memory (bytes) snapshot=1060376576
		Virtual memory (bytes) snapshot=5383991296
		Total committed heap usage (bytes)=903872512
		Peak Map Physical memory (bytes)=568754176
		Peak Map Virtual memory (bytes)=2704990208
	org.apache.hadoop.examples.terasort.TeraGen$Counters
		CHECKSUM=214760662691937609
	File Input Format Counters 
		Bytes Read=0
	File Output Format Counters 
		Bytes Written=10000000000
Spent 427ms computing base-splits.
Spent 3ms computing TeraScheduler splits.
Computing input splits took 430ms
Sampling 10 splits of 38
Making 1 from 100000 sampled records
Computing parititions took 1604ms
Spent 2037ms computing partitions.
WARNING: Use "yarn jar" to launch YARN applications.
22/03/03 06:48:29 INFO terasort.TeraSort: starting
22/03/03 06:48:31 INFO input.FileInputFormat: Total input files to process : 2
22/03/03 06:48:31 WARN impl.MetricsConfig: Cannot locate configuration: tried hadoop-metrics2-xceiverclientmetrics.properties,hadoop-metrics2.properties
22/03/03 06:48:31 INFO impl.MetricsSystemImpl: Scheduled Metric snapshot period at 10 second(s).
22/03/03 06:48:31 INFO impl.MetricsSystemImpl: XceiverClientMetrics metrics system started
22/03/03 06:48:32 INFO client.RMProxy: Connecting to ResourceManager at pra-bup-2.pra-bup.root.hwx.site/172.27.12.195:8032
22/03/03 06:48:33 INFO mapreduce.JobSubmissionFiles: Permissions on staging directory /user/pratyush/.staging are incorrect: rwxrwxrwx. Fixing permissions to correct value rwx------
22/03/03 06:48:34 INFO metrics.MetricRegistries: Loaded MetricRegistries class org.apache.ratis.metrics.impl.MetricRegistriesImpl
22/03/03 06:48:34 INFO mapreduce.JobSubmitter: number of splits:38
22/03/03 06:48:34 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1646195852633_0007
22/03/03 06:48:34 INFO mapreduce.JobSubmitter: Executing with tokens: []
22/03/03 06:48:34 INFO conf.Configuration: resource-types.xml not found
22/03/03 06:48:34 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
22/03/03 06:48:35 INFO impl.YarnClientImpl: Submitted application application_1646195852633_0007
22/03/03 06:48:35 INFO mapreduce.Job: The url to track the job: http://pra-bup-2.pra-bup.root.hwx.site:8088/proxy/application_1646195852633_0007/
22/03/03 06:48:35 INFO mapreduce.Job: Running job: job_1646195852633_0007
22/03/03 06:48:46 INFO mapreduce.Job: Job job_1646195852633_0007 running in uber mode : false
22/03/03 06:48:46 INFO mapreduce.Job:  map 0% reduce 0%
22/03/03 06:49:00 INFO mapreduce.Job:  map 3% reduce 0%
22/03/03 06:49:01 INFO mapreduce.Job:  map 5% reduce 0%
22/03/03 06:49:02 INFO mapreduce.Job:  map 27% reduce 0%
22/03/03 06:49:03 INFO mapreduce.Job:  map 72% reduce 0%
22/03/03 06:49:04 INFO mapreduce.Job:  map 73% reduce 0%
22/03/03 06:49:06 INFO mapreduce.Job:  map 77% reduce 0%
22/03/03 06:49:07 INFO mapreduce.Job:  map 86% reduce 0%
22/03/03 06:49:08 INFO mapreduce.Job:  map 92% reduce 0%
22/03/03 06:49:09 INFO mapreduce.Job:  map 98% reduce 0%
22/03/03 06:49:13 INFO mapreduce.Job:  map 100% reduce 0%
22/03/03 06:49:25 INFO mapreduce.Job:  map 100% reduce 70%
22/03/03 06:49:31 INFO mapreduce.Job:  map 100% reduce 72%
22/03/03 06:49:37 INFO mapreduce.Job:  map 100% reduce 74%
22/03/03 06:49:43 INFO mapreduce.Job:  map 100% reduce 75%
22/03/03 06:49:49 INFO mapreduce.Job:  map 100% reduce 77%
22/03/03 06:49:55 INFO mapreduce.Job:  map 100% reduce 79%
22/03/03 06:50:01 INFO mapreduce.Job:  map 100% reduce 81%
22/03/03 06:50:06 INFO mapreduce.Job:  map 100% reduce 83%
22/03/03 06:50:12 INFO mapreduce.Job:  map 100% reduce 85%
22/03/03 06:50:18 INFO mapreduce.Job:  map 100% reduce 87%
22/03/03 06:50:24 INFO mapreduce.Job:  map 100% reduce 89%
22/03/03 06:50:30 INFO mapreduce.Job:  map 100% reduce 91%
22/03/03 06:50:36 INFO mapreduce.Job:  map 100% reduce 93%
22/03/03 06:50:42 INFO mapreduce.Job:  map 100% reduce 95%
22/03/03 06:50:48 INFO mapreduce.Job:  map 100% reduce 97%
22/03/03 06:50:55 INFO mapreduce.Job:  map 100% reduce 99%
22/03/03 06:50:59 INFO mapreduce.Job:  map 100% reduce 100%
22/03/03 06:50:59 INFO mapreduce.Job: Job job_1646195852633_0007 completed successfully
22/03/03 06:50:59 INFO mapreduce.Job: Counters: 54
	File System Counters
		FILE: Number of bytes read=8621547709
		FILE: Number of bytes written=13025325878
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		OFS: Number of bytes read=10000004636
		OFS: Number of bytes written=10000000000
		OFS: Number of read operations=119
		OFS: Number of large read operations=0
		OFS: Number of write operations=2
	Job Counters 
		Launched map tasks=38
		Launched reduce tasks=1
		Data-local map tasks=34
		Rack-local map tasks=4
		Total time spent by all maps in occupied slots (ms)=715932
		Total time spent by all reduces in occupied slots (ms)=107478
		Total time spent by all map tasks (ms)=715932
		Total time spent by all reduce tasks (ms)=107478
		Total vcore-milliseconds taken by all map tasks=715932
		Total vcore-milliseconds taken by all reduce tasks=107478
		Total megabyte-milliseconds taken by all map tasks=733114368
		Total megabyte-milliseconds taken by all reduce tasks=110057472
	Map-Reduce Framework
		Map input records=100000000
		Map output records=100000000
		Map output bytes=10200000000
		Map output materialized bytes=4393981006
		Input split bytes=4636
		Combine input records=0
		Combine output records=0
		Reduce input groups=100000000
		Reduce shuffle bytes=4393981006
		Reduce input records=100000000
		Reduce output records=100000000
		Spilled Records=296636766
		Shuffled Maps =38
		Failed Shuffles=0
		Merged Map outputs=38
		GC time elapsed (ms)=9498
		CPU time spent (ms)=915240
		Physical memory (bytes) snapshot=27425619968
		Virtual memory (bytes) snapshot=104999276544
		Total committed heap usage (bytes)=25718423552
		Peak Map Physical memory (bytes)=761073664
		Peak Map Virtual memory (bytes)=2710753280
		Peak Reduce Physical memory (bytes)=521015296
		Peak Reduce Virtual memory (bytes)=2693476352
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=10000000000
	File Output Format Counters 
		Bytes Written=10000000000
22/03/03 06:50:59 INFO terasort.TeraSort: done
'''

# arr = st.split("\n")
drago = re.findall(r'The url to track the job: (.*)', st)
print(drago)

counter = 1

for website in drago:
    # Retrieve the HTML
    url = f"{website}"
    html_content = requests.get(url).text

    # Make a soup object for easy parsing
    soup = BeautifulSoup(html_content, "html.parser")

    # Get the needed table
    tera_table = soup.find("table", attrs={"class": "info"})

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
    print(data)

    # write the result into a CSV
    a_file = open(f"results{counter}.csv", "w")

    writer = csv.writer(a_file)
    for key, value in data.items():
        writer.writerow([key, value])

    a_file.close()

    counter += 1
