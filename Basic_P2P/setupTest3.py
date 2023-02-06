from ast import Assert
from asyncio import subprocess
from concurrent.futures import ThreadPoolExecutor
import threading
import unittest
import uuid
from buyer import BuyerClass

from seller import SellerClass


class TestBuyer(unittest.TestCase):
    def runTest(self,seller1, buyer1, buyer2):
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            
            future1 = executor.submit(seller1.registerPeer)
            future2 = executor.submit(buyer1.registerPeer)
            future3 = executor.submit(buyer2.registerPeer)
            print(seller1.item_count)
            print(future1.result)
            self.assertEqual(seller1.item_count,1)
        # if seller1.poll() is not None and buyer1.poll() is not None and buyer2.poll() is not None:
        #     self.assertEqual(seller1.item_count,0)


seller1 = SellerClass(0,1,["Peer1","Peer2"],"salt")
buyer1 = BuyerClass(1,["Peer0"],"salt",0,3)
buyer2 = BuyerClass(2,["Peer0"],"salt",0,3)
TestBuyer().runTest(seller1, buyer1, buyer2)
print(seller1.item_count)    