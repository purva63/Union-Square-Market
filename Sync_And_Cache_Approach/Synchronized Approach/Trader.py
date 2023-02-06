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

class Trader(object):
    def __init__(self, node_id, neighbors, total_nodes):
        self.node_id =  node_id
        self.neighbors = neighbors
        self.total_nodes = total_nodes
        self.requestList = []
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.lock = threading.Lock()
        self.dbServiceId = "Peer100"

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
            #logging.debug('Released a lock for sale')

    #sends isAlive message to register trader so that the peer can consider this node as peer
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
            if len(self.requestList) > 0:
                current_request = self.requestList[0]
                self.requestList = self.requestList[1:]
                
                #logging.info(str(self.node_id)+ " will start processing req"+ str(current_request)+str(self.requestList))
                with Pyro4.Proxy("PYRONAME:"+str(self.dbServiceId)) as peer:
                    peer.getRequest(current_request[0],current_request[1],current_request[2])
        except Exception as e:
            logging.info(str(self.node_id)+' :trader trying to process a new request is an issue' + str(e)) 
