#part 2
import sys
import json
import matplotlib.pyplot as plt

#original_slots = {1:3, 2:5, 3:7}
f = open('Copy of config.json', 'r')
d = json.load(f) 
original_slots = dict()
for i in d['workers']:
    original_slots[i['worker_id']] = i['slots']
f.close()
algo = sys.argv[1]
filename = algo + '_logs2.txt'
f = open(filename, 'r')
lines = f.readlines()
f.close()
graph_data = {}

'''for i in original_conf['worker_id']:
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
    graph_data['time'].append(float(time))'''

for i in range(1, 4):
    graph_data[i] = [[], []]


for i in lines:
    worker_id, slots, time = i.split('\t')
    worker_id = int(worker_id)
    slots = int(slots)
    time = float(time)

    ntasks = original_slots[worker_id]- slots
    graph_data[worker_id][0].append(ntasks)
    graph_data[worker_id][1].append(time)



plt.plot( graph_data[1][1], graph_data[1][0],  linewidth=2, label='worker 1')
plt.plot( graph_data[2][1], graph_data[2][0],  linewidth=2, label='worker 2')
plt.plot( graph_data[3][1], graph_data[3][0],  linewidth=2, label='worker 3')
plt.title('Worker occupancy - ' + algo)
plt.legend()
plt.show()
