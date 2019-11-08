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
        self.SentUpdates = {}           # {tid -> (key, value)}
        self.SentLog = []               # [tid,..]
        
    def initialized(self):
        while not self.Synchronized:
            down_id = self.Ether.waitForDownConnection()
            self.Ether.send("SYNCR", down_id)
            self.sleep(1.0)
            
    def messageReceived(self, t):
        msg = t.Payload
        if not self.Synchronized:
            if t.Dst == self.Ether.ID and msg.startswith("SYNC "):
                data = msg.split(None, 1)[1]
                if data == ".":
                    self.Synchronized = True
                    self.wakeup()
                else:
                    data = json.lods(data)
                    self.Data.update(data)
                    
        elif msg == "SYNCR":
            data = list(self.Data.items())
            n = len(data)
            for i in range(0, n, 10):
                batch = dict(data[i:i+10])
                if batch:
                    self.Ether.send("SYNC %s" % (json.dumps(batch),), t.Src)
            self.Ether.send("SYNC .", t.Src)
            
        elif t.broadcast and msg.startswith("UPDATE "):
            data = msg.split(None, 1)[1]
            data = json.lods(data)
            self.Data.update(data)

    @synchronized
    def messageReturned(self, t):
        inx = self.SentLog.index(t.TID)
        if inx > 0:
            # some messages sent earlier got lost
            for tid in self.SentLog[:inx]:
                name, value = self.SentUpdates.pop(tid)
                self.sendUpdate(name, value)
            self.SentLog = self.SentLog[inx+1:]
                
    def sendUpdate(self, name, value)
        tid = self.Ether.broadcast("UPDATE %s" % (json.dumps({name:value}),))
        self.SentUpdates[tid] = (name, value)
        self.SentLog.append(tid)
        return tid
            
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
        
    

        
