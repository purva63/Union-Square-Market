# Python program to explain os.fork() method 

import os
import random
import shutil
from signal import signal
import subprocess
import sys
import time
import uuid

#total number of nodes is passed as a variable while starting the process
n = int(sys.argv[1])

lst = []
f = open("neighbors.txt", "w")
dict = {0:"fish",1:"boar",2:"salt"}
#make sure there is 1 buyer and seller and the remaining are decided randomly. 
# Additionally, what the buyer will buy or the seller will sell will be chosen randomly
for i in range(int(n)):
    decider = random.randint(0,2)
    if i==1 or (i!=0 and decider==0):
        role = 'buyer'
        args_lst = ["python", role+str(i)+"/"+"Buyer_Seller_Source.py",str(n),str(int(i)),role]
    elif i==0 or decider==1:
        role = 'seller'
        args_lst = ["python", role+str(i)+"/"+"Buyer_Seller_Source.py",str(n),str(int(i)),role]
    else:
        role = 'buyer_seller'
        args_lst = ["python", role+str(i)+"/"+"Buyer_Seller_Source.py",str(n),str(int(i)),role]
    
    os.mkdir(role+str(i))
    shutil.copy('Buyer_Seller_Source.py',role+str(i))
    shutil.copy('Leader.py', role+str(i))
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

    

