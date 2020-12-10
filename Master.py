import sys
from socket import *
import threading
import json
import random
import time
from time import sleep
import pandas as pd

lock_data = threading.Lock()
lock_ack = threading.Lock()
acknowlegment_l = []

# lock_file = threading.Lock()
# data should be global
def send(tkid,dur,port):
    # port=data['port'][ind]
    global algo
    soc=socket(AF_INET,SOCK_STREAM)
    soc.connect(('localhost',port))
    l=[tkid,dur]
    l = " ".join(list(map(str,l)))
    l = l + '\t' + algo
    soc.send(l.encode())
    soc.close()

def check_temp(client_soc , addr):
    val=client_soc.recv(1024)
    val=val.decode()
    #lock
    lock_ack.acquire()
    acknowlegment_l.append(val)
    lock_ack.release()
    #release
    client_soc.close()

def check_acknowledgment():
    soc=socket(AF_INET,SOCK_STREAM)
    print("Socket created succesfully")
    port = 5001
    soc.bind(('localhost',port))
    soc.listen(1)
    print("Listening on port 5001")
    
    while 1:
        client_soc,addr = soc.accept()
        temp1 = threading.Thread(target=check_temp,args=(client_soc,addr))
        temp1.start()

def rand_sch(tkid,dur):
    r=random.randint(0,2)
    t=0
    while t==0:
        #LOCKS REQ
        lock_data.acquire()
        if data['slots'][r]>0:
            data['slots'][r]-=1
            line = str(r + 1) + '\t' + str(data['slots'][r]) + '\t' + str(time.time()) + '\n'
            port=data['port'][r]
            lock_data.release()
            filename = 'RANDOM_logs2.txt'
            f = open(filename,'a')
            f.write(line)
            f.close()
            #lock stop
            thr=threading.Thread(target=send,args=(tkid,dur,port))
            thr.start()
            thr.join()
            #check if acknowledgment
            while 1:
                #lock for ackl
                lock_ack.acquire()
                if(tkid in acknowlegment_l):
                    acknowlegment_l.pop(acknowlegment_l.index(tkid))
                    lock_ack.release()
                    #lock release
                    #lock for data
                    lock_data.acquire()
                    data['slots'][r]+=1
                    line = str(r + 1) + '\t' + str(data['slots'][r]) + '\t' + str(time.time()) + '\n'
                    lock_data.release()
                    filename = 'RANDOM_logs2.txt'
                    f = open(filename,'a')
                    f.write(line)
                    f.close()
                    #release data
                    break
                else:
                    lock_ack.release()
                    #lock release
            t=1
        else:
            #lock stop
            lock_data.release()
            r=random.randint(0,2)

def rr_sch(tkid,dur , task_no):
    t=0
    while t==0:
        #LOCKS REQ
        for i in range(3):
            lock_data.acquire()
            r = (task_no + i) % 3
            if data['slots'][r]>0:
                data['slots'][r]-=1
                line = str(r + 1) + '\t' + str(data['slots'][r]) + '\t' + str(time.time()) + '\n'
                port=data['port'][r]
                lock_data.release()
                filename = 'RR_logs2.txt'
                f = open(filename,'a')
                f.write(line)
                f.close()
                #lock stop
                thr=threading.Thread(target=send,args=(tkid,dur,port))
                thr.start()
                thr.join()
                #check if acknowledgment
                while 1:
                    #lock for ackl
                    lock_ack.acquire()
                    if(tkid in acknowlegment_l):
                        acknowlegment_l.pop(acknowlegment_l.index(tkid))
                        lock_ack.release()
                        #lock release
                        #lock for data
                        lock_data.acquire()
                        data['slots'][r]+=1
                        line = str(r + 1) + '\t' + str(data['slots'][r]) + '\t' + str(time.time()) + '\n'
                        lock_data.release()
                        filename = 'RR_logs2.txt'
                        f = open(filename,'a')
                        f.write(line)
                        f.close()
                        #release data
                        break
                    else:
                        lock_ack.release()
                        #lock release
                t=1
            else:
                #lock stop
                lock_data.release()
            if(t == 1): break
            

def ll_sch(tkid,dur):
    t=0
    while t == 0:
        maxi = 0
        lock_data.acquire()
        for i in range(3):
            #LOCKS REQ
            if data['slots'][i] > data['slots'][maxi]:
                maxi = i
        # lock_data.release()
            #LOCK ENDS
        #LOCKS REQ
        # lock_data.acquire()
        if(data['slots'][maxi] >0): data['slots'][maxi]-=1
        else: 
            lock_data.release()
            continue
        line = str(maxi + 1) + '\t' + str(data['slots'][maxi]) + '\t' + str(time.time()) + '\n'
        port=data['port'][maxi]
        lock_data.release()
        filename = 'LL_logs2.txt'
        f = open(filename,'a')
        f.write(line)
        f.close()
        thr=threading.Thread(target=send,args=(tkid,dur,port))
        thr.start()
        thr.join()
        #LOCK ENDS
        while 1:
            #lock for ackl
            lock_ack.acquire()
            if(tkid in acknowlegment_l):
                acknowlegment_l.pop(acknowlegment_l.index(tkid))
                lock_ack.release()
                #lock release
                #lock for data
                lock_data.acquire()
                data['slots'][maxi]+=1
                line = str(maxi + 1) + '\t' + str(data['slots'][maxi]) + '\t' + str(time.time()) + '\n'
                lock_data.release()
                filename = 'LL_logs2.txt'
                f = open(filename,'a')
                f.write(line)
                f.close()
                #release data
                break
            else:
                lock_ack.release()
                #lock release
        t=1


def select_scheduler(val,algo):           
    map_len=len(val['map_tasks'])
    red_len=len(val['reduce_tasks'])
    m=[]
    rr=0
    for i in range(len(val['map_tasks'])):
        if algo=='RANDOM':
            m.append(threading.Thread(target=rand_sch,args=(val['map_tasks'][i]['task_id'],val['map_tasks'][i]['duration'])))
            m[i].start()
        elif algo=='RR':
            m.append(threading.Thread(target=rr_sch,args=(val['map_tasks'][i]['task_id'],val['map_tasks'][i]['duration'],rr)))
            m[i].start()
            rr+=1
        else:
            m.append(threading.Thread(target=ll_sch,args=(val['map_tasks'][i]['task_id'],val['map_tasks'][i]['duration'])))
            m[i].start()
    for i in range(len(m)):
        m[i].join()
    print('all map tasks are sent and completedof job : ',val['job_id'])
    m=[]
    for i in range(len(val['reduce_tasks'])):
        if algo=='RANDOM':
            m.append(threading.Thread(target=rand_sch,args=(val['reduce_tasks'][i]['task_id'],val['reduce_tasks'][i]['duration'])))
            m[i].start()
        elif algo=='RR':
            m.append(threading.Thread(target=rr_sch,args=(val['reduce_tasks'][i]['task_id'],val['reduce_tasks'][i]['duration'],rr)))
            m[i].start()
            rr+=1
        else:
            m.append(threading.Thread(target=ll_sch,args=(val['reduce_tasks'][i]['task_id'],val['reduce_tasks'][i]['duration'])))
            m[i].start()
    for i in range(len(m)):
        m[i].join()
    job_end = time.time()
    line = 'job_id:' + str(val['job_id']) + '\t' + 'end_time : '+ str(job_end) + '\n'
    # lock_file.acquire()
    filename = algo + '_logs1.txt'
    f = open(filename,'a')
    f.write(line)
    f.close()
    # lock_file.release()
    print('all reduce tasks are sent and completed of job :',val['job_id'])
    #join worker thread

def read(algo):

    soc=socket(AF_INET,SOCK_STREAM)
    print("Socket created succesfully")
    port = 5000
    soc.bind(('localhost',port))
    soc.listen(1)
    print("Listening on port 5000")

    while 1:
        client_soc,addr = soc.accept()

        val=client_soc.recv(1024)
        job_start=time.time()
        client_soc.close()
        val=val.decode()
        val=eval(val)
        line = 'job_id:' + str(val['job_id']) + '\t' + 'start_time : '+ str(job_start) + '\n'
        # lock_file.acquire()
        filename = algo + '_logs1.txt'
        f = open(filename,'a')
        f.write(line)
        f.close()
        # lock_file.release()
        temp = threading.Thread(target=select_scheduler,args=(val,algo)) 
        temp.start()   
        print('Job recieved :',val['job_id'])
       




conf_pa=sys.argv[1]
f = open(conf_pa,)
d = json.load(f) 
f.close()
res = dict()
for i in d['workers']:
    for j in i:
        try:
            res[j].append(i[j])
        except:
            res[j] = []
            res[j].append(i[j])
data = res
algo=sys.argv[2]

filename1 = algo + '_logs1.txt'
f = open(filename1, 'w')
f.close()
filename2 = algo + '_logs2.txt'
f = open(filename2,'w')
f.close()

read_thread = threading.Thread(target=read,args=(algo,))
acknowlegment_thread = threading.Thread(target=check_acknowledgment)
read_thread.start()
acknowlegment_thread.start()



