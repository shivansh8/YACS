import socket
import threading
import time
import sys


def send_acknowledgment(tkid):
    port = 5001
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM )
    soc.connect(('localhost',port))
    soc.send(tkid.encode())
    # end_time = time.time()
    print('Ack sent of :',tkid)
    soc.close()

def check_val(client_soc,w):
    l = client_soc.recv(1024)
    task_arrival = time.time()
    client_soc.close()
    # start_time=time.time()
    l = l.decode()
    x,algo = l.split('\t')
    tkid,dur = x.split()
    print("Task recieved ",tkid,'to worker ',w)
    dur = int(dur)
    time.sleep(dur)
    line = 'task_id:' + str(tkid) + '\t' + 'start_time : '+ str(task_arrival) + '\n'
    filename = algo + '_logs1.txt'
    f = open(filename,'a')
    f.write(line)
    f.close()
    # for i in range(dur):
    #     dur -= 1
    #     time.sleep(1)

    task_endtime = time.time()
    line = 'task_id:' + str(tkid) + '\t' + 'end_time : '+ str(task_endtime) + '\n'
    f = open(filename,'a')
    f.write(line)
    f.close()
    send_acknowledgment(tkid)

def worker1():
    port = int(sys.argv[1])
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM )
    soc.bind(('localhost',port))
    soc.listen(1)
    while 1:
        client_soc , addr = soc.accept()
        t1 = threading.Thread(target=check_val,args=(client_soc,sys.argv[2]))
        t1.start()



inp1 = threading.Thread(target=worker1)
inp1.start()
