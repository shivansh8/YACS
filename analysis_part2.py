#part 2
import sys
import json
import matplotlib.pyplot as plt


f = open('Copy of config.json', 'r')
d = json.load(f) 
original_conf = dict()
for i in d['workers']:
    for j in i:
        try:
            original_conf[j].append(i[j])
        except:
            original_conf[j] = []
            original_conf[j].append(i[j])
f.close()
filename = sys.argv[1] + '_logs2.txt'
f = open(filename, 'r')
lines = f.readlines()
f.close()
graph_data = {}
for i in original_conf['worker_id']:
    graph_data[i] = []
graph_data['time'] = []


for i in lines:
    d, time = i.split('\t')
    d = eval(d)
    slots = d['slots']
    worker_id = d['worker_id']

    for j,k in zip(worker_id, slots):
        ntasks = original_conf['slots'][j-1] - k
        graph_data[j].append(ntasks)
    graph_data['time'].append(float(time))

plt.plot( graph_data['time'], graph_data[1],  linewidth=2, label='worker 1')
plt.plot( graph_data['time'], graph_data[2],  linewidth=2, label='worker 2')
plt.plot( graph_data['time'], graph_data[3],  linewidth=2, label='worker 3')
plt.legend()
plt.show()