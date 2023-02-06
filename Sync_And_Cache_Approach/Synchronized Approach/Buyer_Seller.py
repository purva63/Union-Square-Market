from asyncio.log import logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Process
from Trader import Trader
import sys
from threading import Thread
import time
import Pyro4
import logging
from datetime import datetime
import random
import subprocess
import os
import signal

@Pyro4.expose
class Buyer_Seller(Trader):
    def __init__(self, node_id, neighbors, total_nodes,sellerItem, role, noTraders):
        self.neighbors = neighbors
        self.node_id = node_id
        self.total_nodes = total_nodes
        self.role = role
        self.sellItem = sellerItem
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.traderList = []
        self.isTrader = False
        self.noTraders = noTraders

    #register the trader wth the nodes so that they can connect with the trader with requests of buy or sell
    def registerTrader(self, traderId):
        #logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
        self.traderList.append("Peer"+str(traderId))
        #logging.info(str(self.node_id) + ": trader " + str(traderId)+" added to the trader list. Trader list now is:" + str(self.traderList))
    
    #sending buy / sell request to trader by choosing 1 trader randomly amongst all the traders
    def requestTrader(self,action,itemId, item, quantity):
        if self.isTrader == True:
            super().requestTrader(action,itemId, item, quantity)

    #the node was made the leader and the leader was registered.
    def makeTrader(self):
        logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
        try:
            #logging.info(str(self.node_id)+": inside makeTrader")
            self.neighbors = []
            for i in range(int(self.total_nodes)):
                if self.node_id != i:
                    self.neighbors.append("Peer"+str(i))

            self.sellItem = None
            self.isTrader = True
        except Exception as e:
            logging.info(str(self.node_id)+' :making a trader is an issue. '+ str(e)) 
    
    #this is to start election. 2 traders with max node_id will be elected as traders
    def startElection(self, visited):
        logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
        try:
            if self.node_id not in visited:
                visited.append(self.node_id)
                for neighbor in self.neighbors:
                    #logging.info(str(self.node_id)+ " : in start election. sending message to " + str(neighbor) + ":"+ str(visited))
                    with Pyro4.Proxy("PYRONAME:"+str(neighbor)) as peer:
                        visited = peer.startElection(visited)
            return visited
        except Exception as e:
            logging.info(str(self.node_id)+' :starting election is an issue. '+ str(e)) 

    #create new requests of buy or sell from time to time.
    def generateRequest(self):
        logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
        try:
            if len(self.traderList) > 0:
                trader = random.choice(self.traderList)
                dict = {0:"fish",1:"boar",2:"salt"}
                
                #logging.info(str(self.node_id) + " request generated for " + str(trader))
                if self.role == "buyer":
                    item = random.randint(0,2)
                    with Pyro4.Proxy("PYRONAME:"+str(trader)) as peer:
                        peer.requestTrader("buyer",item, dict[item], random.randint(1,5))
                else:
                    with Pyro4.Proxy("PYRONAME:"+str(trader)) as peer:
                        peer.requestTrader("seller",self.sellItem, dict[self.sellItem], random.randint(1,5))
        except Exception as e:
            logging.info(str(self.node_id)+' :generating a request is an issue. '+ str(e)) 
    
    #register this peer with the naming server and either start election or start requests if traders were elected.
    def registerPeer(self):
        logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
        try:
            with Pyro4.Daemon(host="127.0.0.1", port=(5000+self.node_id)) as daemon:
                ns = Pyro4.locateNS()
                uri = daemon.register(self)
                ns.register("Peer"+str(self.node_id), uri)
                timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                logging.info(timestamp+"Peer"+str(self.node_id)+" uri="+ str(uri))
                logging.info("Before req loop "+str(self.node_id))
                future1 = self.executor.submit(daemon.requestLoop)

                if self.node_id == 0:
                    temp = self.startElection([])
                    temp.sort()

                    traderTemp = temp[-self.noTraders:]
                    for trader in traderTemp:
                        with Pyro4.Proxy("PYRONAME:Peer"+str(trader)) as peer:
                            peer.makeTrader()
                    
                
                while True:
                    if self.isTrader == True:
                        super().__init__(self.node_id,self.neighbors,self.total_nodes)
                        super().startTrader()
                    if len(self.traderList) > 0 and self.isTrader == False:
                        future2 = self.executor.submit(self.generateRequest)

        except Exception as e:
            logging.info(str(self.node_id)+' :Buyer - unable to get the result'+ str(e)) 

if __name__ == '__main__':
    total_nodes = int(sys.argv[1])
    node_id = int(sys.argv[2])
    role = sys.argv[3]
    
    neighbors =[]
    neighbors.append("Peer"+str( (node_id+1 )%total_nodes))
    neighbors.append("Peer"+str( (total_nodes+ node_id-1 )%total_nodes))

    buyer = Buyer_Seller(node_id, neighbors, total_nodes,random.randint(0,2), role,2)
    buyer.registerPeer()