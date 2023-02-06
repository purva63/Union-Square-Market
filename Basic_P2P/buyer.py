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
import uuid

@Pyro4.expose
class BuyerClass(object):
    #For the buyer class the constructor consists of itemToBuy which changes every call with initial item passed 
    def __init__(self, node_id, neighbors, itemToBuy, hop_cnt,total_nodes):
        self.hop_cnt = hop_cnt
        self.neighbors = neighbors
        self.itemToBuy = itemToBuy
        self.node_id = node_id
        self.total_nodes = total_nodes
        self.process_result = []
        self.completed_requests = set()
        self.item_count = 0
        self.total_requests = 0
        self.executor = ThreadPoolExecutor(max_workers=15)
    """
    The exposed method lookup for each buyer is called by itself or its peers
    The method defaults to and always sends the request forward so other sellers can be notified aswell
    The method does not return anything
    A lookup consists of a uid - A unique id generated by a buyer for a transaction for an item
    lst is a stack to backtrack on reply for the sellers, hopcount and itemToBuy are set globally and the buyer respectively
    """
    def lookup(self,uid,hop_count,lst,itemToBuy):
        try:
            logging.basicConfig(filename='example_buyer.log',level=logging.DEBUG)
            logging.info(str(uid)+" :Buyer in floodLookup for Peer"+str(self.node_id)+" :"+str(lst)+":"+ str(self.neighbors))
            
            if hop_count > 0:
                for neighbor in self.neighbors:
                    logging.info("Buyer "+str(self.node_id) +": Now trying for "+ neighbor)
                    #Do not send flood lookups back to it's parent, this method is shortcircuited when call by self. 
                    if lst == [] or lst[-1]!=neighbor:
                        with Pyro4.Proxy("PYRONAME:"+neighbor) as peer:
                            new_lst = lst.copy()
                            new_lst.append("Peer"+str(self.node_id))
                            timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                            logging.info(timestamp+" -"+str(uid)+" Lookup for "+ str(itemToBuy)+" sending from Peer"+str(self.node_id)+" to "+ str(neighbor) + ":"+ str(new_lst))
                            #multithreaded lookup
                            future = self.executor.submit(peer.lookup,uid,hop_count-1, new_lst, itemToBuy)
                    elif lst == []:
             
                        logging.info(str(uid)+" :lst is empty in Seller")
                    else:
     
                        logging.info(str(uid)+" :lst top is parent in seller"+" :"+str(lst)+":"+ str(neighbor))
            else:
        
                logging.info(str(uid)+" :"+str(self.node_id)+" has hop count"+ str(hop_count))
        except Exception as e:
            logging.info(str(uid)+' :Buyer '+ str(self.node_id)  +'flood Unable to get the result :'+str(lst)+":"+str(e))
   
    """
    The reply method is triggered by a lookup only at the seller
    This method when reaches a buyer with an empty stack represents the reply is intended for the buyer
    and queues the response in it's local list of transaction replies to be processed
    This method consists of the unique transaction id for the buyer for a request so buyer can initiate final transaction
    The sellerid is used by the buyer to find the peer directly from naming register
    The list is used to backtrack to the original buyer who flooeded to the seller
    """
    def reply(self, uid, sellerId, lst):
        try:
            logging.basicConfig(filename='example_buyer.log',level=logging.DEBUG)
            #empty stack means message intended for this node
            if len(lst) == 0 :
                logging.info("IN reply. Reply received. Checking UID- self:"+str(uid)+":"+str(lst))
                timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                logging.info(timestamp+" -"+str(uid)+"  found match. Buying from: Peer"+str(self.node_id)+":"+str(lst))
                #Add to transactions to be processed list on reply to original buyer
                self.process_result.append(["Peer"+str(sellerId),str(uid)])
            #Forwarding reply to intended pper
            else:
                if len(lst) > 0:
                    to_send = lst.pop()
                    with Pyro4.Proxy("PYRONAME:"+to_send) as peer:
                        new_lst = lst.copy()
                        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        logging.info(timestamp+" -"+str(uid)+" Reply sending from: Peer"+str(self.node_id) + "to "+to_send+":"+str(new_lst))
                        future = self.executor.submit(peer.reply,uid,sellerId, new_lst)
                else:
                    logging.info(str(uid)+" List unexpected size=0, Peer:" + str(self.node_id)+":"+str(new_lst))
        except Exception as e:
            logging.info(str(uid)+' :Buyer '+ str(self.node_id)  +'unable to reply :'+str(lst)+":"+str(e)) 
    """
    The buy mehtod established connection to the seller. Once the connection is establed, it connects to the seller using its naming server
    and then on getting the lock to the seller item, the seller reduced the item count by 1.
    """
    def buy(self,uid, sellerId):
        logging.basicConfig(filename='example_buyer.log',level=logging.DEBUG)
        logging.info("IN buy. Reply received. Checking UID- self:"+str(uid))

        with Pyro4.Proxy("PYRONAME:"+str(sellerId)) as peer:
            timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            isSuccessful = peer.contactSeller(self.node_id,sellerId,self.itemToBuy, uid)
            logging.info(timestamp+" -"+str(uid)+" Peer"+str(self.node_id)+" bought from "+str(sellerId)+" : "+str(isSuccessful))
            if isSuccessful:
                logging.info("Changed uid at Peer"+str(self.node_id) +" to "+ str(uid))
                logging.info("Buyer Peer" + str(self.node_id) +"will now buy" + self.itemToBuy)
                rev_dict = {"fish":0,"boar":1,"salt":2}
                dict = {0:"fish",1:"boar",2:"salt"}
                curr_item = rev_dict[self.itemToBuy]
                self.itemToBuy = dict[(curr_item+1)%3]
    """
    The register peer method starts threads to listen for requests on the exposed object. 
    It registers itself with the node id at the naming register and requestLoop runs infinitely listening
    to requests
    """
    def registerPeer(self):
        logging.basicConfig(filename='example_buyer.log',level=logging.DEBUG)
        try:
            with Pyro4.Daemon(host="127.0.0.1", port=(5000+self.node_id)) as daemon:
                ns = Pyro4.locateNS()
                uri = daemon.register(self)
                ns.register("Peer"+str(self.node_id), uri)
                timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                logging.info(timestamp+"Peer"+str(self.node_id)+" uri="+ str(uri))
                logging.info("Before req loop "+str(self.node_id))
                future1 = self.executor.submit(daemon.requestLoop)
                future2 = self.executor.submit(self.checkResult)
                while True:
                    self.total_requests += 1
                    logging.info(str(self.node_id)+" - Number of requests created" + str(self.total_requests))
                    logging.info(str(self.node_id)+" - Number of success" + str(self.item_count))
                    uid = uuid.uuid1()
                    time.sleep(7)
                    future = self.executor.submit(self.lookup, uid,self.hop_cnt,[],self.itemToBuy)

                    rev_dict = {"fish":0,"boar":1,"salt":2}
                    dict = {0:"fish",1:"boar",2:"salt"}
                    curr_item = rev_dict[self.itemToBuy]
                    self.itemToBuy = dict[(curr_item+1)%3]
        except Exception as e:
            logging.info('Buyer Unable to get the result'+ str(e))

    """
    This function makes sure only 1 transaction is done for each request. All the servers that respond to the request are inserted in the list
    The first request is considered and the remaining are discarded. A set is maintained to keep track of all the previous transactions cleared.
    """
    def checkResult(self):
        logging.basicConfig(filename='example_buyer.log',level=logging.DEBUG)
        while True:
            time.sleep(10)
            new_list = [x for x in self.process_result if x[1] not in self.completed_requests]
            self.process_result = new_list

            logging.info("got some results:" + str(self.process_result))

            for ele in self.process_result:
                if ele[1] not in self.completed_requests:
                    self.buy(ele[1],ele[0])
                    self.completed_requests.add(ele[1])
                    self.item_count +=1

if __name__ == '__main__':
    total_nodes = int(sys.argv[1])
    node_id = int(sys.argv[2])
    hop_cnt = max(int(sys.argv[3]),1)
    dict = {0:"fish",1:"boar",2:"salt"}
    itemToBuy = dict[random.randint(0,2)]
    
    if total_nodes == 2:
        neighbors = [sys.argv[4]]
    else:
        neighbors = [sys.argv[4],sys.argv[5]]
    
    buyerClass = BuyerClass(node_id, neighbors, itemToBuy, hop_cnt,total_nodes)
    buyerClass.registerPeer()
