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
        self.NeedReceipt = set()
        
    def init(self):
        self.DownLink.start()
        self.DiagonalLink.start()
        #self.UpLink.start()
        
    def uplinkConnectedJ(self, j):
        print("Link: uplink connected to", self.UpNodes[j])
        diagonals = self.UpNodes[j+1:-1]    # exclude the uplink and self
        self.DiagonalLink.setDiagonals(diagonals)
        
    @synchronized
    def send(self, payload, to=None, send_diagonal=True, cross_diagional=True):
        t = Transmission(self.Index, to, payload, send_diagonal, cross_diagional)
        tid = t.TID
        self.Seen.set(tid, (False, True, False))
        self.UpLink.send(t)
        if send_diagonal:
            self.Seen.set(tid, (False, True, True))
            self.DiagonalLink.send(t)

    @synchronized
    def processTransmission(self, t, from_diagonal):
        #print("processTransmission", t.TID, t.Src, t.Dst, t.Payload, from_diagonal)
        tid = t.TID
        seen_received, sent_up, sent_diag = self.Seen.get(tid, (False, False, False))

        forward = True
        
        if not seen_received:
            if t.Dst == self.Index or t.is_broadcast:
                forward = self.messageReceived(t.Src, t.Dst, t.Payload) != False
                seen_received = True
        if forward and t.Dst != self.Index:
            if not sent_up and (not from_diagonal or t.cross_diagonal):
                sent_up = True
                self.UpLink.send(t)
            
            if not sent_diag and (from_diagonal or t.cross_diagonal):
                sent_diag = True
                self.DiagonalLink.send(t)
                
        self.Seen.set(tid, (seen_received, sent_up, sent_diag))
            
    def messageReceived(self, src, dst, msg_bytes):
        pass