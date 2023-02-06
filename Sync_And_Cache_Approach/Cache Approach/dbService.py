import logging
import json
import Pyro4
from datetime import datetime
import concurrent.futures
import threading
#from concurrent.futures import ThreadPoolExecutor, wait

@Pyro4.expose
class dbService(object):
    def __init__(self):
        self.salt = 0
        self.boar = 0
        self.fish = 0
        self.node_id = 100
        self.lock = threading.Lock()
        self.noOfReq = 0
        self.traderList = []
        #self.executor = ThreadPoolExecutor(max_workers=15)

    #this method processes requests and updates the database
    def getRequest(self,requestType,item,quantity,salt,boar,fish,aliveTraders):
        logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
        self.lock.acquire()
        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        logging.info(timestamp+"DB Service no of Req completed --"+str(self.noOfReq))
        try:
            self.salt = salt - (len(aliveTraders))*self.salt
            self.boar = boar - (len(aliveTraders))*self.boar
            self.fish = fish - (len(aliveTraders))*self.fish

            #logging.info("Db Service: Lock acquired. Request coming in was" + str(requestType) + ":" + str(item) + ":" + str(quantity))
            if requestType == 'buyer':
                if (item == 'salt' and quantity > self.salt) or (item == 'boar' and quantity > self.boar) or (item == 'fish' and quantity > self.fish):
                    logging.info("DB Service: the request could not be processed as the stock is not enough" + ":" + item 
                    +". Stock was salt:" + str(self.salt) + ": boar:" + str(self.boar) + ": fish:" + str(self.fish))
                    self.noOfReq += 1
        
                    return [self.salt,self.boar,self.fish]
                quantity = quantity * -1
            
            if item == 'salt':
                self.salt += quantity
            if item == 'boar':
                self.boar += quantity
            if item == 'fish':
                self.fish += quantity

            dictionary = {
                "salt" : self.salt,
                "boar" : self.boar,
                "fish" : self.fish
            }

            json_object = json.dumps(dictionary, indent=3)
            
            with open("sample.json", "w") as outfile:
                outfile.write(json_object)
            
            logging.info("DB Service: request was processes. The stock now is" + str(json_object))
            return [self.salt,self.boar,self.fish]
        except Exception as e:
            logging.info(' db service: issue with storing in file : ' + str(e)) 
        finally:
            self.lock.release()

    #adds the peer to the trader list so that it can be labelled as a trader
    def addTrader(self, node_id):
        logging.basicConfig(filename='example_peer.log',level=logging.DEBUG)
        self.traderList.append(node_id)
        logging.info("In db service trader list: Trader list is now: "+str(self.traderList))
        return 

    #register this db service with the naming server
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
                #future1 = 
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(daemon.requestLoop)
                #wait(future1)

        except Exception as e:
            logging.info(str(self.node_id)+' :DB Service unable to get the result'+ str(e)) 



if __name__ == '__main__':
    dbservice = dbService()
    dbservice.registerPeer()