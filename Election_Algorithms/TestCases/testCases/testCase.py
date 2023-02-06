from ast import Assert
from asyncio import subprocess
from concurrent.futures import ThreadPoolExecutor
import threading
import unittest
import uuid
from Buyer_Seller_Source import BuyerSellerClass


class TestBuyer(unittest.TestCase):
    def runTest(self,buyerSeller0, buyerSeller1, buyerSeller2):
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            
            future1 = executor.submit(buyerSeller0.registerPeer)
            future2 = executor.submit(buyerSeller1.registerPeer)
            future3 = executor.submit(buyerSeller2.registerPeer)

        # if seller1.poll() is not None and buyer1.poll() is not None and buyer2.poll() is not None:
        #     self.assertEqual(seller1.item_count,0)
#node_id, neighbors, itemToBuy, itemToSell, total_nodes, role
buyerSeller0 = BuyerSellerClass(0,["Peer2","Peer1"],None, "salt",3,"seller")
buyerSeller1 = BuyerSellerClass(1,["Peer2","Peer0"],None, None,3,"buyer")
buyerSeller2 = BuyerSellerClass(2,["Peer1","Peer0"],None, None,3,"buyer")
TestBuyer().runTest(buyerSeller0, buyerSeller1, buyerSeller2)  