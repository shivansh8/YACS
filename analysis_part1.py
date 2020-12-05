import numpy as np
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import sys
import json

#part 1
df = {'job_mean': [], 'job_median': [], 'task_mean': [], 'task_median': []}
f = open('LL_logs1.txt', 'r')
lines = f.readlines()
f.close()
jobs = {}
tasks = {}
for i in lines:
    id, time = i.split('\t')
    if ('job' in id):
        id = id.split(':')
        try:
            jobs[id[1]] = float(time.split(':')[1]) - jobs[id[1]]
        except:
            jobs[id[1]] = float(time.split(':')[1])
    else:
        id = id.split(':')
        try:
            tasks[id[1]] = float(time.split(':')[1]) - tasks[id[1]]
        except:
            tasks[id[1]] = float(time.split(':')[1])


job_log = list(jobs.values())
job_log = np.array(job_log)
task_log = np.array(list(tasks.values()))

df['job_mean'].append(np.mean(job_log))
df['job_median'].append(np.median(job_log))

df['task_mean'].append(np.mean(task_log))
df['task_median'].append(np.median(task_log))
# ll_logs = []
# ll_logs.append(np.mean(job_log))
# ll_logs.append(np.median(job_log))

# ll_logs.append(np.mean(task_log))
# ll_logs.append(np.median(task_log))


f = open('RR_logs1.txt', 'r')
lines = f.readlines()
f.close()
jobs = {}
tasks = {}
for i in lines:
    id, time = i.split('\t')
    if ('job' in id):
        id = id.split(':')
        try:
            jobs[id[1]] = float(time.split(':')[1]) - jobs[id[1]]
        except:
            jobs[id[1]] = float(time.split(':')[1])
    else:
        id = id.split(':')
        try:
            tasks[id[1]] = float(time.split(':')[1]) - tasks[id[1]]
        except:
            tasks[id[1]] = float(time.split(':')[1])


job_log = list(jobs.values())
job_log = np.array(job_log)
task_log = np.array(list(tasks.values()))

df['job_mean'].append(np.mean(job_log))
df['job_median'].append(np.median(job_log))

df['task_mean'].append(np.mean(task_log))
df['task_median'].append(np.median(task_log))
# rr_logs = []
# rr_logs.append(np.mean(job_log))
# rr_logs.append(np.median(job_log))

# rr_logs.append(np.mean(task_log))
# rr_logs.append(np.median(task_log))


f = open('RANDOM_logs1.txt', 'r')
lines = f.readlines()
f.close()
jobs = {}
tasks = {}
for i in lines:
    id, time = i.split('\t')
    if ('job' in id):
        id = id.split(':')
        try:
            jobs[id[1]] = float(time.split(':')[1]) - jobs[id[1]]
        except:
            jobs[id[1]] = float(time.split(':')[1])
    else:
        id = id.split(':')
        try:
            tasks[id[1]] = float(time.split(':')[1]) - tasks[id[1]]
        except:
            tasks[id[1]] = float(time.split(':')[1])


job_log = list(jobs.values())
job_log = np.array(job_log)
task_log = np.array(list(tasks.values()))

df['job_mean'].append(np.mean(job_log))
df['job_median'].append(np.median(job_log))

df['task_mean'].append(np.mean(task_log))
df['task_median'].append(np.median(task_log))

# random_logs = []
# random_logs.append(np.mean(job_log))
# random_logs.append(np.median(job_log))

# random_logs.append(np.mean(task_log))
# random_logs.append(np.median(task_log))
job_mean = df['job_mean']
job_median = df['job_median']
task_mean = df['task_mean']
task_median = df['task_median']


# print(rr_logs,random_logs,ll_logs, sep='\n')
fig = plt.subplots(figsize =(11, 6))
barWidth = 0.18
r1 = np.arange(len(job_mean))
r2 = [x + barWidth for x in r1]
r3 = [x + barWidth for x in r2]
r4 = [x + barWidth for x in r3]

plt.bar(r1, job_mean, width=barWidth, edgecolor='white', label='job_mean')
plt.bar(r2, job_median,  width=barWidth, edgecolor='white', label='job_median')
plt.bar(r3, task_mean,  width=barWidth, edgecolor='white', label='task_mean')
plt.bar(r4, task_median, width=barWidth, edgecolor='white', label='task_median')

plt.title('Analyze part1',fontweight='bold')
plt.xticks([r + barWidth for r in range(3)], ['RANDOM', 'RR', 'LL'])
plt.legend()
plt.show()