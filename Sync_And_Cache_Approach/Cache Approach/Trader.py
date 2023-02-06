from asyncio.log import logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Process
import sys
import threading
import time
import Pyro4
import logging
from datetime import datetime
import random
import subprocess
import os
import signal
import json

class Trader(object):
    def __init__(self, node_id, neighbors, total_nodes, trader_list):
        self.node_id =  node_id
        self.neighbors = neighbors
        self.total_nodes = total_nodes
        self.requestList = []
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.lock = threading.Lock()
        self.dbServiceId = "Peer100"
        self.salt = 0
        self.boar = 0
        self.fish = 0
        self.traderList = trader_list
        self.inMidReq = False
        self.initTime = time.time()
        self.totalReqProc = 0

    #this method gets stock from other traders
    def getStock(self):
        #logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
        self.inMidReq = True
        #logging.info(str(self.node_id)+" has stock as follow: salt---" + str(self.salt) + ":boar --- " + str(self.boar) + ": fish---" + str(self.fish))
        if self.node_id == 4 and time.time() - self.initTime > 5:
            return False
        return [self.salt,self.boar,self.fish]
    
    #register stock as your own stock
    def registerStock(self,salt,boar,fish):
        self.salt = salt
        self.fish = fish
        self.boar = boar
        self.inMidReq = False
    
    #mark this peer as a trader so that other peers can send requests
    def startTrader(self):
        logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
        self.aliveMessage()
        while True:
            future1 = self.executor.submit(self.processRequest)

    #appends the request to the queue of the trader
    def requestTrader(self,action,itemId, item, quantity):
        self.lock.acquire()
        logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
        try:
            self.requestList.append([action,item,quantity])
            #logging.info(str(self.node_id) + "added request to the list. Request List now is:" + str(self.requestList))
        except Exception as e:
            logging.info(str(self.node_id)+' :leader unable to append request'+ str(e)) 

        finally:
            self.lock.release()
            logging.debug('Released a lock for sale')

    #sends isAlive message to other traders so that the peer is not considered dead
    def aliveMessage(self):
        logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
        try:
            for neighbor in self.neighbors:
                #logging.info(str(self.node_id) + "sending alive message to " + str(neighbor))
                with Pyro4.Proxy("PYRONAME:"+str(neighbor)) as peer:
                        peer.registerTrader(self.node_id)
        except Exception as e:
            logging.info(str(self.node_id) + "issue with sending alive message" + str(e))

    #processes requests that have been received
    def processRequest(self):
        logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
        try:
            save_dict = {"requestList":self.requestList, "salt":self.salt, "boar":self.boar, "fish":self.fish,"neighbors":self.neighbors}
            json_object = json.dumps(save_dict)
            
            with open("Peer"+str(self.node_id)+"replicafile.json", "w") as outfile:
                outfile.write(json_object)
            #logging.info("request file saved")
        except:
            logging.info("Could not save request file")
        try:
            self.totalReqProc +=1
            timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            logging.info(timestamp+"TRADER " + str(self.node_id)+" no of Req completed --"+str(self.totalReqProc))
            if len(self.requestList) > 0 and self.inMidReq == False:
                current_request = self.requestList[0] #requestType,item,quantity
                self.requestList = self.requestList[1:]
                curr_stock = [self.salt,self.boar,self.fish]
                fin_stock = [self.salt,self.boar,self.fish]
                if current_request[0] == "buyer":
                    
                    if (current_request[1] == 'salt' and current_request[2] > self.salt) or (current_request[1] == 'boar' and current_request[2] > self.boar) or (current_request[1] == 'fish' and current_request[2] > self.fish):
                        #logging.info(str(self.node_id)+ " current req is of buyer"+ str(self.traderList))
                        self.inMidReq = True
                        aliveTraders=[]
                        removelist =[]
                        removeneighbors=[]
                        for otherTraders in self.traderList:
                            #logging.info(str(self.node_id)+ " in other traderList loop"+ str(otherTraders))
                            with Pyro4.Proxy("PYRONAME:"+str(otherTraders)) as peer:
                                curr_stock = peer.getStock()
                                if curr_stock:
                                    self.salt += curr_stock[0]
                                    self.boar += curr_stock[1]
                                    self.fish += curr_stock[2]
                                    aliveTraders.append(otherTraders)
                                else:
                                    removelist.append(otherTraders)
                                    json_dict = json.loads(str(otherTraders)+"replicafile.json")
                                    self.requestList+=json_dict["requestList"]
                                    removeneighbors.append(json_dict["neighbors"])
                            #logging.info(str(self.node_id)+ " after other traderList loop -- "+ str(self.salt) + ":" + str(self.boar) + ":" + str(self.fish))
                        for rem in removelist:
                            self.traderList.remove(rem)

                        #logging.info("Alive traders are: "+str(aliveTraders))
                        #logging.info(str(self.node_id)+ " will start processing req --"+ str(current_request)+str(self.requestList))
                        with Pyro4.Proxy("PYRONAME:"+str(self.dbServiceId)) as peer:
                            fin_stock = peer.getRequest(current_request[0],current_request[1],current_request[2],self.salt,self.boar,self.fish,aliveTraders)
                        self.salt = fin_stock[0]
                        self.boar = fin_stock[1]
                        self.fish = fin_stock[2]

                        for rem,neighbors in zip(removelist,removeneighbors):
                            self.traderList.remove(rem)
                            for neighbor in neighbors:
                                with Pyro4.Proxy("PYRONAME:"+str(neighbor)) as peer:
                                    peer.removeTrader(str(neighbor))


                        for otherTraders in self.traderList:
                            with Pyro4.Proxy("PYRONAME:"+str(otherTraders)) as peer:
                                peer.registerStock(self.salt,self.boar,self.fish)

                        self.inMidReq = False
                    else:
                        
                        if current_request[1] == "salt":
                            self.salt -= current_request[2]
                        if current_request[1] == "fish":
                            self.fish -= current_request[2]
                        if current_request[1] == "boar":
                            self.boar -= current_request[2]
                        #logging.info(str(self.node_id) + "buy req processed as it had sufficient funds -- "+ str(self.salt) + ":" + str(self.boar) + ":" + str(self.fish))

                else:
                    if current_request[1] == "salt":
                        self.salt += current_request[2]
                    if current_request[1] == "fish":
                        self.fish += current_request[2]
                    if current_request[1] == "boar":
                        self.boar += current_request[2]
                    #logging.info(str(self.node_id) + "sell req processed as it had sufficient funds -- "+ str(self.salt) + ":" + str(self.boar) + ":" + str(self.fish))

                
        except Exception as e:
            logging.info(str(self.node_id)+' :trader trying to process a new request is an issue' + str(e)) 
