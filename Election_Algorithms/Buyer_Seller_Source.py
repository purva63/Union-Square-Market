from asyncio.log import logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Process
from Leader import Leader
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
class BuyerSellerClass(Leader):
    #constructor to initialize default values
    def __init__(self, node_id, neighbors, itemToBuy, itemToSell, total_nodes, role):
        self.neighbors = neighbors
        self.itemToSell = itemToSell
        self.itemToBuy = itemToBuy
        self.node_id = node_id
        self.total_nodes = total_nodes
        self.quantity_to_sell = None
        self.quantity_to_buy = None
        self.item_count = 0
        self.role = role
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.leader = None
        self.isResponse = True
        self.price_of_item = None
        self.total_requests = 0
        self.seller_profit = 0
        self.timestamp = [0]*total_nodes
        self.electionStarted = False
        self.timestampOfLastElection = datetime.now()
        self.timesLastSaleReq = datetime.now()

    #check if the node is already a leader
    def isLeader(self):
        return False

    #the node has to register the leader so that the node can connect to the leader for every request / new item selling
    def registerLeader(self,leader_id, timestamp):
        try:
            logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
            self.leader = "Peer"+str(leader_id)
            self.timestamp[self.node_id]+=1
            self.timestamp = [max(x,y) for x,y in zip(self.timestamp,timestamp) ]
            self.timestampOfLastElection = datetime.now()
            logging.info(str(self.node_id)+ " with timestamp"+ str(self.timestamp) + " - leader is registered as" + str(self.leader))
        except Exception as e:
            logging.info(str(self.node_id)+' :registering leader is an issue' + str(e)) 

    #when buyer needs to buy a new item and connect to the leader to register the request
    def buy(self):
        logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
        dict = {0:"fish",1:"boar",2:"salt"}
        try:
            self.total_requests += 1
            self.isResponse = False
            itemIndex = random.randint(0,2)
            self.itemToBuy = dict[itemIndex]
            self.quantity_to_buy = random.randint(1,5)
            logging.info(str(self.node_id)+ " with timestamp"+ str(self.timestamp) + " - IN buy. creating a new request for leader" + str(self.leader))

            with Pyro4.Proxy("PYRONAME:"+str(self.leader)) as peer:
                self.timestamp[self.node_id] +=1
                peer.contactLeader(self.node_id,itemIndex, self.quantity_to_buy,self.timestamp)
                logging.info((datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])+" -"+ str(self.node_id)+ " with timestamp"+ str(self.timestamp) 
                + " - request sent to the leader for" + str(self.itemToBuy)+":"+str(self.quantity_to_buy))
                self.isResponse = True
        except Exception as e:
            logging.info(str(self.node_id)+' :buy / creating a new request an issue' + str(e)) 
    
    #when seller needs to seller a new item and connect to the leader to register the seller and its item to sell
    def sell_new_item(self):
        logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
        dict = {0:"fish",1:"boar",2:"salt"}
        try:
            self.timesLastSaleReq = datetime.now()
            item_no = random.randint(0,2)
            self.itemToSell = dict[item_no]
            self.price_of_item = random.randint(10,20)
            self.quantity_to_sell = random.randint(5,11)
            logging.info(str(self.node_id)+" trying to register new item with leader: " + str(self.itemToSell) + ":"+ str(self.leader))
            with Pyro4.Proxy("PYRONAME:"+str(self.leader)) as peer:
                peer.registerItem(self.node_id,item_no, self.quantity_to_sell)
                logging.info((datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])+" -"+ str(self.node_id)+ " with timestamp"+ str(self.timestamp) + 
                " - seller decided to sell new item " + str(item_no)+":"+str(self.quantity_to_sell))
        except Exception as e:
            logging.info(str(self.node_id)+' :selling new item is an issue' + str(e)) 
    
    #the leader will contact the seller to complete a buy request and the balance for seller is updated
    def contactSeller(self, quantity):
        try:
            logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
            logging.info((datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])+" -"+ str(self.node_id)+ " with timestamp"+ str(self.timestamp) + 
                    " - selling item" +":"+ str(self.itemToSell) +":"+ str(self.quantity_to_sell) +". requested for:"+str(quantity))
            if self.itemToSell == None or self.quantity_to_sell == None or self.quantity_to_sell < quantity:
                return False

            self.quantity_to_sell -= quantity
            self.seller_profit += quantity * self.price_of_item

            logging.info((datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])+" -"+ str(self.node_id)+ "one more request processed")

            return True
        except Exception as e:
            logging.info(str(self.node_id)+' :selling item to leader is an issue' + str(e)) 

    #the nodes start an election if a leader does not exists or the alive message was not received for more than 5 seconds
    def startElection(self, visited):
        
        self.electionStarted = True
        if self.node_id not in visited:
            visited.append(self.node_id)
            for neighbor in self.neighbors:
                with Pyro4.Proxy("PYRONAME:"+str(neighbor)) as peer:
                    visited = peer.startElection(visited)
        return visited
    
    #the node was made the leader and the leader was registered.
    def makeLeader(self):
        logging.info(str(self.node_id)+": inside makeLeader")
        neighbors = []
        for i in range(int(self.total_nodes)):
            if self.node_id != i:
                neighbors.append("Peer"+str(i))

        sellerDict = {0:{},1:{},2:{}}
        super().__init__(self.node_id, neighbors, sellerDict, self.total_nodes)
        super().registerPeer()

    
    """
        The register peer method starts threads to listen for requests on the exposed object. 
        It registers itself with the node id at the naming register and requestLoop runs infinitely listening
        to requests
        
    """
    def registerPeer(self):
        logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
        rev_dict = {"fish":0,"boar":1,"salt":2}
        dict = {0:"fish",1:"boar",2:"salt"}
        try:
            with Pyro4.Daemon(host="127.0.0.1", port=(5000+self.node_id)) as daemon:
                ns = Pyro4.locateNS()
                uri = daemon.register(self)
                ns.register("Peer"+str(self.node_id), uri)
                timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                logging.info(timestamp+"Peer"+str(self.node_id)+" uri="+ str(uri))
                logging.info("Before req loop "+str(self.node_id))
                future1 = self.executor.submit(daemon.requestLoop)
                future2 = None
                future3 = None
                while True:
                    if self.node_id == 0 and (self.leader == None or ((datetime.now() - self.timestampOfLastElection).total_seconds() > 5 
                    and self.electionStarted == False)):
                        visited = self.startElection([])
                        self.timestampOfLastElection = datetime.now()
                        logging.info(str(self.node_id)+" - election for leader finished" + str(visited))
                        leaderNode = max(visited)
                        
                        if leaderNode >= 0:
                            self.leader = "Peer"+str(leaderNode)
                            leaderPort = (5000+leaderNode)
                            alreadyLeader = False

                            with Pyro4.Proxy("PYRONAME:"+str(self.leader)) as peer:
                                alreadyLeader = peer.isLeader()
                            logging.info(str(self.node_id)+" - is leader finished" + str(alreadyLeader))

                            if alreadyLeader == False:
                                logging.info(str(self.node_id)+"killed process elected as leader" + str(self.node_id)+":"+str(future2)+":"+str(future3))
                                if future2 is not None:
                                    was_cancelled1 = future2.cancel()
                                if future3 is not None:
                                    was_cancelled2 = future3.cancel()
                                with Pyro4.Proxy("PYRONAME:"+str(self.leader)) as peer:
                                    #logging.info("BEFORE "+str(self.node_id)+"killed process elected as leader" + str(self.node_id)+":"+str(future2)+":"+str(future3))
                                    peer.makeLeader()
                                    logging.info(str(self.node_id)+"killed process elected as leader" + str(self.node_id)+":")
                
                    if self.leader != None and (self.role == 'buyer' or self.role == 'buyer_seller'):
                        future2 = self.executor.submit(self.buy)

                    if self.leader != None and (self.role == 'seller' or self.role == 'buyer_seller') and (self.itemToSell == None or 
                    (datetime.now() - self.timesLastSaleReq).total_seconds() > 10):
                        future3 = self.executor.submit(self.sell_new_item)

                    #time.sleep(3)
        except Exception as e:
            logging.info(str(self.node_id)+' :Buyer '+ str(self.node_id)  +'unable to get the result'+ str(e)) 
        

if __name__ == '__main__':
    total_nodes = int(sys.argv[1])
    node_id = int(sys.argv[2])
    dict = {0:"fish",1:"boar",2:"salt"}

    role = sys.argv[3]
    res = None
    
    neighbors =[]
    neighbors.append("Peer"+str( (node_id+1 )%total_nodes))
    neighbors.append("Peer"+str( (total_nodes+ node_id-1 )%total_nodes))

    itemToBuy = None
    itemToSell = None
    if role == 'seller' or role == 'buyer_seller':
        itemToSell = dict[random.randint(0,2)]

    buyerClass = BuyerSellerClass(node_id, neighbors, itemToBuy, itemToSell, total_nodes, role)
    buyerClass.registerPeer()