from ast import Assert
from asyncio import subprocess
from concurrent.futures import ThreadPoolExecutor
import threading
import unittest
import uuid
from Buyer_Seller import Buyer_Seller


class TestBuyer(unittest.TestCase):
    def runTest(self,buyerSeller0, buyerSeller1, buyerSeller2, buyerSeller3, buyerSeller4, buyerSeller5):
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            
            future1 = executor.submit(buyerSeller0.registerPeer)
            future2 = executor.submit(buyerSeller1.registerPeer)
            future3 = executor.submit(buyerSeller2.registerPeer)
            future3 = executor.submit(buyerSeller3.registerPeer)
            future3 = executor.submit(buyerSeller4.registerPeer)
            future3 = executor.submit(buyerSeller5.registerPeer)

        # if seller1.poll() is not None and buyer1.poll() is not None and buyer2.poll() is not None:
        #     self.assertEqual(seller1.item_count,0)
#node_id, neighbors, itemToBuy, itemToSell, total_nodes, role
buyerSeller0 = Buyer_Seller(0,["Peer1","Peer5"],6,0,"buyer",2)
buyerSeller1 = Buyer_Seller(1,["Peer0","Peer2"],6,0,"buyer",2)
buyerSeller2 = Buyer_Seller(2,["Peer1","Peer3"],6,0,"buyer",2)
buyerSeller3 = Buyer_Seller(3,["Peer2","Peer4"],6,0,"buyer",2)
buyerSeller4 = Buyer_Seller(4,["Peer3","Peer5"],6,0,"buyer",2)
buyerSeller5 = Buyer_Seller(5,["Peer0","Peer4"],6,0,"buyer",2)
TestBuyer().runTest(buyerSeller0, buyerSeller1, buyerSeller2,buyerSeller3,buyerSeller4,buyerSeller5)  

# super().__init__(self.node_id,self.neighbors,self.total_nodes)
# super().startTrader()
# buyer = Buyer_Seller(node_id, neighbors, total_nodes, role)