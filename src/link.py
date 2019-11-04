from pythreader import PyThread, Primitive, synchronized
from SockStream import SockStream
from socket import *
import random, time
from transmission import Transmission
from uplink import UpLink
from downlink import DownLink
from diaglink import DiagonalLink

class SeenMemory(Primitive):
    def __init__(self, capacity, lowwater=None):
            Primitive.__init__(self)
            if lowwater is None:    lowwater = int(capacity*0.9)
            assert lowwater < capacity
            self.Capacity = capacity
            self.LowWater = lowwater
            self.Memory = {}            # tid -> (time, data)

    @synchronized
    def get(self, k, default=None):
        tup = self.Memory.get(k)
        if tup is not None:
            t, data = tup
            self.Memory[k] = (time.time(), data)
            return data
        else:
            return default

    @synchronized
    def set(self, k, data):
        self.Memory[k] = (time.time(), data)
        if len(self.Memory) >= self.Capacity:
            lst = sorted(self.Memory.items(), key=lambda x: x[1][0], reversed=True)
            self.Memory = dict(lst[:self.LowWater])

class Link(Primitive):
    
    def __init__(self, inx, nodes):
        Primitive.__init__(self)
        self.Index = inx
        self.Nodes = nodes          # [(ip, ip_bind, port),...]
        _, ip_bind, port = nodes[inx]
        up_nodes = [(i,) + (ip, port) for i, (ip, _, port) in enumerate(nodes)]
        up_nodes = (up_nodes[inx:] + up_nodes[:inx])[::-1]  # sorted in upwards order
        self.UpNodes = up_nodes
        self.Seen = SeenMemory(10000)               # data: (seen, sent_edge, sent_diagonal)
        self.DiagonalLink = DiagonalLink(self, ip_bind, port)
        self.DownLink = DownLink(self, inx, ip_bind, port)
        self.UpLink = UpLink(self, inx, self.UpNodes)
        
    @staticmethod
    def flags(**args):
        return Transmission.flags(**args)
        
    def init(self):
        print("Link.init()")
        self.DownLink.start()
        self.DiagonalLink.start()
        self.UpLink.init()
        
    def uplinkConnected(self):
        print("Link: uplink connected to", self.UpNodes[j])
        diagonals = self.UpNodes[j+1:-1]    # exclude the uplink and self
        self.DiagonalLink.setDiagonals(diagonals)
        
    @synchronized
    def send(self, payload, to=Transmission.BROADCAST, flags=None):
        if flags is None:   flags = Link.flags()        # defaults
        t = Transmission(self.Index, to, payload, flags)
        tid = t.TID
        self.Seen.set(tid, (False, True, False))
        self.UpLink.send(t)
        if t.send_diagonal:
            self.Seen.set(tid, (False, True, True))
            self.DiagonalLink.send(t)

    @synchronized
    def routeTransmission(self, t, from_diagonal):
        #print("processTransmission", t.TID, t.Src, t.Dst, t.Payload, from_diagonal)
        tid = t.TID
        seen, sent_up, sent_diag = self.Seen.get(tid, (False, False, False))

        forward = True
        
        if not seen:
            if t.broadcast:
                if t.mutable:
                    payload = self.processMessage(t.Src, t.Dst, t.Payload)
                    if payload is not None:
                        t.Payload = payload
                    else:
                        forward = False
                else:
                    forward = self.messageReceived(t.Src, t.Dst, t.Payload) != False
            elif t.Dst == self.Index:
                self.messageReceived(t.Src, t.Dst, t.Payload)
                forward = False
            seen = True
            
        if forward and (t.broadcast or t.Dst != self.Index):
            if not sent_up and t.send_edge and (not from_diagonal or t.cross_to_diagonal):
                sent_up = True
                self.UpLink.send(t)
            
            if not sent_diag and t.send_diagonal and (from_diagonal or t.cross_to_diagonal):
                sent_diag = True
                self.DiagonalLink.send(t)
                
        self.Seen.set(tid, (seen, sent_up, sent_diag))
        
    #
    # virtual
    #
            
    def messageReceived(self, src, dst, msg_bytes):
        # returns False to stop forwarding
        pass
        
    def processMessage(self, src, dst, msg_bytes):
        # possibly mutate message
        # return new msg_bytes or None to stop forwarding
        pass