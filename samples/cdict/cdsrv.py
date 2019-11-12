from ring import EtherLink, to_str, to_bytes
from pythreader import Primitive, synchronized
import yaml, sys, getopt, time, random, os, json
from pythreader import PyThread

class CDTimeoutError(Exception):
    pass

class CDServer(Primitive):
    
    def __init__(self, ether):
        Primitive.__init__(self)
        self.Data = {}
        self.Ether = ether
        self.Synchronized = False
        self.PendingUpdates = {}        # key -> value
        self.ID = self.MyRank = self.Ether.ID     # use the Ether ID (hex string) as rank
        
        
    @synchronized
    def initialized(self):
        while not self.Synchronized:
            #print("CDServer.initialize: waiting for down connection...")
            down_id = self.Ether.waitForDownConnection()
            if down_id == self.ID:
                # I am the only one on the network. Initialize to empty data.
                self.Synchronized = True
            else:
                self.Ether.send("SYNCR", down_id)
                self.sleep(1.0)
        #print("CDServer: initialized")
            
    @synchronized
    def messageReceived(self, t):
        msg = t.Payload
        cmd, rest = msg.split(None, 1) if ' ' in msg else (msg, "")
        #print ("messageReceived:", t)
        #print ("messageReceived:", cmd, rest)
        #print ("messageReceived: upID:", self.Ether.upID)
        
        if not self.Synchronized:
            if cmd == "SYNC":
                assert t.Dst == self.ID             # SYNC is always sent from immediate down connection
                data = rest
                if data == ".":
                    self.Synchronized = True
                    self.wakeup()
                else:
                    data = json.loads(data)
                    self.Data.update(data)
                    
        elif cmd == "SYNCR" and t.Src == self.Ether.upID: # SYNC is always sent from immediate down connection to immediate up
            data = list(self.Data.items())
            n = len(data)
            for i in range(0, n, 10):
                batch = dict(data[i:i+10])
                if batch:
                    # SYNC is always sent from immediate down connection to immediate up
                    self.Ether.send("SYNC %s" % (json.dumps(batch),), t.Src, send_diagonal=False) 
            self.Ether.send("SYNC .", t.Src, send_diagonal=False)
            #print("messageReceived: SYNC data sent")
            
        elif cmd == "UPDATE":
            # update from some other node. If its rank is higher than mine, update my pending changes and pass
            # the update unchanged ...
            data = json.loads(rest)
            self.Data.update(data)
            if self.PendingUpdates is not None:
                if t.Src > self.MyRank:
                    for k in self.PendingUpdates.keys():
                        if k in data:
                            self.PendingUpdates[k] = data[k]
                else:
                    # if my rank is higher, alter the data with my values and pass it along
                    for k, v in self.PendingUpdates.keys():
                        if k in data:
                            data[k] = v
                    return "UPDATE %s" % (json.dumps(data),)
            
    @synchronized
    def messageReturned(self, t):
        msg = t.Payload
        cmd, rest = msg.split(None, 1) if ' ' in msg else (msg, "")
        if cmd == "UPDATE":
            if t.TID == self.PendingUpdateTID:
                data = json.loads(rest)
                self.Data.update(data)
                self.PendingUpdates = None      # signal that the transaction is done
                self.wakeup()
            else:
                # ignore
                pass

    @synchronized
    def __setitem__(self, name, value):
        assert isinstance(value, (int, float, str, None))
        self.update({name:value})
        
    def __getitem__(self, key):
        return self.Data[key]
        
    def get(self, key, default=None):
        return self.Data.get(key, default)
        
    @synchronized
    def update(self, dct, tmo=None):
        # syncronous version
        if len(dct) == 0:   return {}
        t1 = None if tmo is None else time.time() + tmo
        self.PendingUpdates = dct.copy()
        resend_interval = 1.0
        done = False
        while not done and self.PendingUpdates is not None and (t1 is None or time.time() < t1):
            encoded = json.dumps(self.PendingUpdates)
            self.PendingUpdateTID = self.Ether.broadcast("UPDATE %s" % (encoded,), 
                        fast=False, guaranteed=True, mutable=True)
            tresend = time.time() + resend_interval
            
            # wait for the broadcast to return
            while (t1 is None or time.time() < t1) and time.time() < tresend and self.PendingUpdates is not None:
                self.sleep(resend_interval)
            if self.PendingUpdates is None :
                done = True
            elif t1 is not None and time.time() > t1:
                break
        if not done:
            raise CDTimeoutError()
        out = {k:self.Data[k] for k in dct.keys()}
        return out
        
if __name__ == "__main__":
    
    ether = EtherLink(sys.argv[1])
    cd = CDServer(ether)
    ether.init(cd)
    
    while True:
        cmd = input("> ")
        if "=" in cmd:
            dct = {}
            for part in cmd.split(","):
                part = part.strip()
                name, value = part.split("=", 1)
                name = name.strip()
                value = value.strip()
                dct[name] = value
            out = cd.update(dct)
            for k, v in out.items():
                print(">> %s = %s" % (k, v))
        else:
            name = cmd.strip()
            print (">> %s = %s" % (name, cd.get(name)))
        
        
