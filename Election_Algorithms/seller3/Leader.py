from asyncio.log import logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Process
import sys
from threading import Thread
import time
import Pyro4
import logging
from datetime import datetime
import random
import pickle
import threading
import os.path

@Pyro4.expose
class Leader(object):
    #constructor to initialize default values
    def __init__(self, node_id, neighbors, sellerDict, total_nodes):
        self.neighbors = neighbors
        self.sellerDict = sellerDict
        self.node_id = node_id
        self.total_nodes = total_nodes
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.timestamp = [0]*total_nodes
        self.request_list = []
        self.lock = threading.Lock()
        self.lastAliveMessage = None
        self.resultWrite = None
        self.timeForFirstReq = datetime.now()
        self.totalReqProcessed = 0

        if os.path.isfile('saved_dictionary.pkl'):
            self.load()

    #store leader details in the file in case the leader crashes
    def save(self):
        try:
            fileData = self.sellerDict
            fileData["request_list"] = self.request_list
            logging.info(str(self.node_id)+"saving file"+ str(fileData))
            with open('saved_dictionary.pkl', 'wb') as f:
                pickle.dump(fileData, f)
        except Exception as e:
            logging.info(str(self.node_id)+' :saving is an issue' + str(e)) 
        
    #the next leader should pick up from where the previous had stopped, the requests shoul not be lost
    def load(self):
        with open('saved_dictionary.pkl', 'rb') as f:
            loaded_dict = pickle.load(f)
            self.request_list = loaded_dict.pop("request_list")
            self.sellerDict = loaded_dict

    #check if the node is already a leader
    def isLeader(self):
        return False

    #the leader is processing one req at a time from the list, lock has been applied to make sure multiple threads do not
    #access the request list at the same time. To avoid deadlock.
    def processRequest(self):
        logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
        self.lock.acquire()
        try:
            if len(self.request_list)>0:
                current_request = self.request_list[0]
                logging.info(str(self.node_id)+ " leader with timestamp"+ str(self.timestamp) + "will start processing req"+ str(current_request)+": dict:"+ str(self.sellerDict)+":"+str(self.request_list))
                self.request_list = self.request_list[1:]
                for key in self.sellerDict[current_request[1]].keys():
                    logging.info(str(self.node_id)+ " leader with timestamp"+ str(self.timestamp) + " - finding seller" + str(key) +":: requirement"+ str(current_request[2])+":"+str(self.request_list))
                    quantity = self.sellerDict[current_request[1]][key]

                    if quantity >= current_request[2]:
                        with Pyro4.Proxy("PYRONAME:Peer"+str(key)) as peer:
                            result = peer.contactSeller,current_request[2]
                            if result == False:
                                self.request_list.insert(0,current_request)
                            
                        break
                if len(self.request_list) == 0 or self.request_list[0] != current_request:
                    self.totalReqProcessed += 1
                    timeToProcess = datetime.now() - self.timeForFirstReq
                    with open(str(self.total_nodes)+'_nodes_1000Req.txt', 'a+') as f:
                        f.write(str(self.totalReqProcessed)+","+str(datetime.now())+"\n")
                    
                    logging.info(str(self.node_id)+"  - processed one more request" + str(self.totalReqProcessed)+":"+str(self.totalReqProcessed)+","+str(timeToProcess))

                if self.lastAliveMessage is not None and (datetime.now() - self.lastAliveMessage).total_seconds() > 2:
                    self.save()
        except Exception as e:
            logging.info(str(self.node_id)+' :leader trying to process a new request is an issue' + str(e)) 
        finally:
            self.lock.release()
            #logging.debug('Released a lock to process req')

    #new requests are added to the request list when buyers create them
    def contactLeader(self,buyer_id, itemToBuy, quantity_to_buy, timestamp):
        logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
        self.lock.acquire()
        try:
            #logging.info(str(self.node_id)+ " leader with timestamp"+ str(self.timestamp) + " - before processing request" + ":"+ str(self.request_list))
            self.timestamp[self.node_id]+=1
            self.timestamp = [max(x,y) for x,y in zip(self.timestamp,timestamp) ]
            self.request_list.append([buyer_id,itemToBuy,quantity_to_buy])
            logging.info(str(self.node_id)+ " leader with timestamp"+ str(self.timestamp) + " - after processing request" + ":"+ str(self.request_list))

        except Exception as e:
            logging.info(str(self.node_id)+' :leader unable to append request'+ str(e)) 

        finally:
            self.lock.release()
            logging.debug('Released a lock for sale')
            
    #register the node as leader and start threads to process requests and send alive messages
    def registerPeer(self):
        logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
        try:
            logging.info("Before req loop "+str(self.node_id))
            while True:
                if self.lastAliveMessage == None or (datetime.now() - self.lastAliveMessage).total_seconds() > 5:
                    self.lastAliveMessage = datetime.now()
                    future2 = self.executor.submit(self.aliveMessage)
                future3 = self.executor.submit(self.processRequest)
        
        except Exception as e:
            logging.info('leader Unable to get the register leader'+ str(e))

    #send alive message to all nodes sending timestamps and letting nodes know the leader is alive
    def aliveMessage(self):
        logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
        try:
            logging.info(str(self.node_id)+" will send alive message to neighbors:" + str(self.neighbors))
            for neighbor in self.neighbors:
                self.lastAliveMessage = datetime.now()
                logging.info("Just checking in leader" + str(neighbor))
                with Pyro4.Proxy("PYRONAME:"+str(neighbor)) as peer:
                    logging.info(str(self.node_id)+' :leader sending alive message '+ str(neighbor))
                    peer.registerLeader(self.node_id,self.timestamp)
        except Exception as e:
            logging.info(str(self.node_id)+' :sending alive message is an issue' + str(e)) 

    #when a seller decides to sell new item instead of the previous
    #the leader updates its data for this seller
    def registerItem(self, seller_id, item_no, quantity_to_sell):
        logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
        try:
            self.sellerDict[item_no].update({seller_id:quantity_to_sell})
            self.sellerDict[(item_no+1)%3].update({seller_id:0})
            self.sellerDict[(item_no+2)%3].update({seller_id:0})
            logging.info(str(self.node_id)+' :registering item for seller '+ str(seller_id)) 
        except Exception as e:
            logging.info(str(self.node_id)+' :leader registering item is an issue' + str(e)) 

if __name__ == '__main__':
    total_nodes = int(sys.argv[1])
    node_id = int(sys.argv[2])
    
    neighbors = []
    for i in range(int(total_nodes)):
        if node_id != i:
            neighbors.append("Peer"+str(i))

    sellerDict = {0:{},1:{},2:{}}

    leaderClass = Leader(node_id, neighbors, sellerDict, total_nodes)
    leaderClass.registerPeer()