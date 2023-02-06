# Python program to explain os.fork() method 

#subprocess.call('cd buyer1', shell=True)
import os
import random
import shutil
from signal import signal
import subprocess
import sys
import time
import uuid

n = int(sys.argv[1])
#Hop count is set to n/2 -1 since the maximum distance in a circular network is n/2
max_hop_count = int(n/2 - 1)
#randomly select global sell count for the sellers
max_items = random.randint(3,6)
lst = []
f = open("neighbors.txt", "w")
dict = {0:"fish",1:"boar",2:"salt"}
#make sure there is 1 buyer and seller and the remaining are decided randomly. 
# Additionally, what the buyer will buy or the seller will sell will be chosen randomly
for i in range(int(n)):
    decider = random.randint(0,9)
    if i==1 or (i!=0 and decider%2!=0):
        role = 'buyer'
        args_lst = ["python", role+str(i)+"/"+role+".py",str(n),str(int(i)),str(max_hop_count)]
    else:
        role = 'seller'
        args_lst = ["python", role+str(i)+"/"+role+".py",str(n),str(int(i)),str(max_items)]

    if i == 0:
        args_lst.append("Peer"+str(i+1))
        if n!=2 :
            args_lst.append("Peer"+str(n-1))  
    elif i == n-1:
        if n!=2:
            args_lst.append("Peer"+str(0))
        args_lst.append("Peer"+str(i-1))
    else:
        args_lst.append("Peer"+str(i-1))
        args_lst.append("Peer"+str(i+1))

    if role == 'seller':
        args_lst.append(dict[random.randint(0,2)])
    
    os.mkdir(role+str(i))
    shutil.copy(role+'.py',role+str(i))
    time.sleep(2)
    print(args_lst)
    #Each buyer and seller is a subprocess 
    completed_process = subprocess.Popen(args_lst)
    lst.append(completed_process)
    f.write(str(i)+",Node"+str(i)+","+role+",127.0.0.1,"+str(5000+i)+",Peer"+str(i)+"\n")
f.close()

while(True):
    a = input("Enter X to terminate: ")
    if a == "X" or a == "x":
        for i in lst:
            #This command is used to kill all the spawned subprocesses and their children
            subprocess.call(['taskkill', '/F', '/T', '/PID', str(i.pid)])
        break
        # try:
        #     print(os.getcwd)
        #     os.rmdir(os.path.join(str(os.getcwd), "seller0"))
        #     #C:\Users\purva\Downloads\UMass_677\Lab1_P2P\seller0
        # except OSError as e:
        #     print("Error: " + str(e.strerror))


    

