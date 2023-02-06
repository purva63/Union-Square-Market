from ast import Assert
from asyncio import subprocess
from concurrent.futures import ThreadPoolExecutor
import threading
import unittest
import uuid
from Buyer_Seller import Buyer_Seller
import random

class TestBuyer(unittest.TestCase):
    def runTest(self,buyerSeller0, buyerSeller1, buyerSeller2, buyerSeller3,buyerSeller4,buyerSeller5,buyerSeller6,buyerSeller7):
        
        with ThreadPoolExecutor(max_workers=30) as executor:
            
            future1 = executor.submit(buyerSeller0.registerPeer)
            future2 = executor.submit(buyerSeller1.registerPeer)
            future3 = executor.submit(buyerSeller2.registerPeer)
            future4 = executor.submit(buyerSeller3.registerPeer)
            future5 = executor.submit(buyerSeller4.registerPeer)
            future6 = executor.submit(buyerSeller5.registerPeer)
            future7 = executor.submit(buyerSeller6.registerPeer)
            future8 = executor.submit(buyerSeller7.registerPeer)

        # if seller1.poll() is not None and buyer1.poll() is not None and buyer2.poll() is not None:
        #     self.assertEqual(seller1.item_count,0)
#node_id, neighbors, itemToBuy, itemToSell, total_nodes, role
buyerSeller0 = Buyer_Seller(0,["Peer1","Peer5"],8,random.randint(0,2),"seller",2)
buyerSeller1 = Buyer_Seller(1,["Peer0","Peer2"],8,random.randint(0,2),"seller",2)
buyerSeller2 = Buyer_Seller(2,["Peer1","Peer3"],8,random.randint(0,2),"seller",2)
buyerSeller3 = Buyer_Seller(3,["Peer2","Peer4"],8,random.randint(0,2),"seller",2)
buyerSeller4 = Buyer_Seller(4,["Peer3","Peer5"],8,random.randint(0,2),"seller",2)
buyerSeller5 = Buyer_Seller(5,["Peer4","Peer0"],8,random.randint(0,2),"buyer",2)
buyerSeller6 = Buyer_Seller(6,["Peer5","Peer7"],8,random.randint(0,2),"seller",2)
buyerSeller7 = Buyer_Seller(7,["Peer6","Peer0"],8,random.randint(0,2),"buyer",2)
TestBuyer().runTest(buyerSeller0, buyerSeller1, buyerSeller2,buyerSeller3,buyerSeller4,buyerSeller5,buyerSeller6,buyerSeller7)  

# super().__init__(self.node_id,self.neighbors,self.total_nodes)
# super().startTrader()
# buyer = Buyer_Seller(node_id, neighbors, total_nodes, role)