from ring import EtherLink, to_str, to_bytes
from pythreader import Primitive
import yaml, sys, getopt, time, random, os, json
from pythreader import PyThread

class CDServer(Primitive):
    
    def __init__(self, ether):
        Primitive.__init__(self)
        self.Data = {}
        self.Ether = ether
        self.Synchronized = False
        self.PendingUpdates = {}        # key -> value
        
    def initialized(self):
        while not self.Synchronized:
            down_id = self.Ether.waitForDownConnection()
            self.Ether.send("SYNCR", down_id)
            self.sleep(1.0)
            
    def messageReceived(self, t):
        msg = t.Payload
        cmd, rest = msg.split(None, 1) if ' ' in msg else (msg, "")
        if not self.Synchronized:
            if cmd == "SYNC":
                assert t.Dst == self.ID             # SYNC is always sent from immediate down connection
                data = rest
                if data == ".":
                    self.Synchronized = True
                    self.wakeup()
                else:
                    data = json.lods(data)
                    self.Data.update(data)
                    
        elif cmd == "SYNCR" and t.Str == self.Ether.upID: # SYNC is always sent from immediate down connection to immediate up
            data = list(self.Data.items())
            n = len(data)
            for i in range(0, n, 10):
                batch = dict(data[i:i+10])
                if batch:
                    # SYNC is always sent from immediate down connection to immediate up
                    self.Ether.send("SYNC %s" % (json.dumps(batch),), t.Src, send_diagonal=False) 
            self.Ether.send("SYNC .", t.Src, send_diagonal=False)
            
        elif cmd == "UPDATE":
            data = json.lods(rest)
            self.Data.update(data)
            
        elif cmd == "LOCK":
            if self.PendingUpdates:
                self.sendUpdates()
                return "LOCK %s" % (self.Ether.ID,)         # send the lock back to me
            self.HaveLock = False
            # lock will be forwarded now
            
    @synchronized
    def sendUpdates(self):
        data = json.dumps(self.PendingUpdates)
        self.Ether.broadcast("UPDATE %s" % (data,))        
    
    def messageReturned(self, t):
        msg = t.Payload
        cmd, rest = msg.split(None, 1) if ' ' in msg else (msg, "")
        if cmd == "LOCK":       # my own lock request
            self.HaveLock = True
            if self.PendingUpdates:
                self.sendUpdates()      # resend them
                self.Ether.broadcast("LOCK %s" % (self.Ether.ID,)) 

        elif cmd == "UPDATE":       # my own update returned
            data = json.lods(rest)
            for k, v in data.items():
                if k in self.PendingUpdates:
                    del self.PendingUpdates[k]
                self.Data[k] = v

    @synchronized
    def __setitem__(self, name, value):
        assert isinstance(value, (int, float, str, None))
        self.sendUpdate(name, value)
        self.Data[name] = value
        
    def __getitem__(self, key):
        return self.Data[key]
        
    def get(self, key, default=None):
        return self.Data.get(key, default)

    @synchronized
    def confirmed(self, key):
        if not key in self.Data:
            raise KeyError(key)
        for k, v in self.SentUpdates.values():
            if k == key:
                return False
        else:
            return True
    
    @synchronized
    def waitForConfirmation(self, key, tmo=None)
        while not self.confirmed(key):
            self.sleep(tmo)
    

        
