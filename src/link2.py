from pythreader import PyThread, Primitive, synchronized
from socket import *
import random, time, uuid, yaml
from .transmission import Transmission
from .uplink2 import UpLink
from .downlink3 import DownLink
from .diaglink import DiagonalLink

from .version import Version

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

class Poller(PyThread):
    def __init__(self, link):
        PyThread.__init__(self)
        self.Link = link
        
    def run(self):
        while True:
            self.Link._send_system_poll()
            time.sleep(10.0+random.random())
        self.Link = None
        
def read_config(path_or_file):
    if isinstance(path_or_file, str):
        path_or_file = open(path_or_file, "r")
    cfg = yaml.load(path_or_file, Loader=yaml.SafeLoader)
    cfg["nodes"] = list(map(tuple, cfg["nodes"]))
    #print("read_config: nodes:", cfg["nodes"])
    return cfg
    
class EtherLinkDelegate(object):    # virtual base class for delegates
    
    def messageReceived(self, t):
        pass
        
    def messageReturned(self, t):
        pass
        
    def initialized(self, node_id):
        pass
        
    def downConnected(self, node_id, addr):
        pass
        
    def upConnected(self, node_id, addr):
        pass
        

class EtherLink(Primitive):
    
    Version = Version
    
    def __init__(self, cfg, delegate = None):
        Primitive.__init__(self)
        config = read_config(cfg)
        nodes = config["nodes"]
        self.Delegate = delegate
        self.ID = uuid.uuid1().hex
        self.Seen = SeenMemory(10000)               # data: (seen, sent_edge, sent_diagonal)
        self.DownLink = DownLink(self, nodes)
        inx = self.DownLink.Index
        up_nodes = nodes
        if inx is not None:
            up_nodes = nodes[inx+1:] + nodes[:inx+1]
        self.UpLink = UpLink(self, up_nodes)
        self.DiagonalLink = DiagonalLink(self, self.DownLink.Address)
        self.Map = []
        self.Poller = Poller(self)
        self.Initialized = False
        
    @property
    def downID(self):
        return self.DownLink.downLinkID
        
    @property
    def upID(self):
        return self.UpLink.upLinkID
        
    @staticmethod
    def flags(**args):
        return Transmission.flags(**args)
        
    def init(self, delegate = None):
        #print("Link.init()")
        self.Delegate = delegate
        self.DownLink.start()
        self.UpLink.start()
        self.DiagonalLink.start()
        self.Poller.start()
        if self.Delegate is not None and hasattr(self.Delegate, "initialized"):
            self.Delegate.initialized(self.ID)
        self.Initialized = True
            
    def run(self, delegate=None):
        if not self.Initialized:
            self.init(delegate)
        self.DownLink.join()
        
    def shutdown(self):
        self.UpLink.shutdown()
        self.DownLink.shutdown()
        #print("EtherLink.shutdown: waiting for down link...")
        self.DownLink.join()
        #print("EtherLink.shutdown: waiting for up link...")
        self.UpLink.join()
        
    def downLinkAddress(self):
        return self.DownLink.Address

    @synchronized
    def _transmit(self, t):
        tid = t.TID
        self.Seen.set(tid, (False, True, False))
        #print("_transmit: sending...")
        self.UpLink.send(t)
        #print("_transmit: sent up: %s" % (t,))
        if t.send_diagonal:
            self.Seen.set(tid, (False, True, True))
            self.DiagonalLink.send(t)
        return tid

    def send(self, payload, to, system=False, send_diagonal=True):
        assert to != Transmission.BROADCAST, "Use EtherLink.broadcast() to send broadcast messages"
        t = Transmission(self.ID, to, payload, send_diagonal = send_diagonal, system=system)
        return self._transmit(t)

    def broadcast(self, payload, fast=False, guaranteed=False, mutable=False, system=False):
        t = Transmission(self.ID, Transmission.BROADCAST, payload, 
            send_diagonal = fast, cross_to_edge = not guaranteed, mutable=mutable, system=system)
        return self._transmit(t)

    @synchronized
    def routeTransmission(self, t, from_diagonal):
        
        #print("routeTransmission: from_diagonal=%s %s" % (from_diagonal, t))

        tid = t.TID
        seen, sent_up, sent_diag = self.Seen.get(tid, (False, False, False))

        forward = True
        if not seen:
            forward = True
            seen = True
            if t.broadcast or t.Dst == self.ID:
                ret = None
                if t.system:
                    ret = self._process_system_message(t)
                else:
                    if t.broadcast \
                                and t.Src == self.ID:
                        #print("routeTransmission: runnerReturned")
                        if self.Delegate is not None:
                            self.Delegate.messageReturned(t)
                    else:
                        if self.Delegate is not None:
                            ret = self.Delegate.messageReceived(t)
                if ret is not None and t.mutable:
                    t.Payload = ret

            if t.broadcast:
                forward = (t.Src != self.ID)
            else:
                forward = (t.Dst != self.ID)
            
        if forward:
            if not sent_up and t.send_edge and (not from_diagonal or t.cross_to_diagonal):
                sent_up = True
                self.UpLink.send(t)
            
            if not sent_diag and t.send_diagonal and (from_diagonal or t.cross_to_diagonal):
                sent_diag = True
                self.DiagonalLink.send(t)
                
        self.Seen.set(tid, (seen, sent_up, sent_diag))
        
    def _process_system_message(self, t):
        #print("_process_system_message:",t.Flags,t.Payload)
        payload = t.Payload
        if payload.startswith(".POLL"):
            return self._process_system_poll(t)
            
    def _process_system_poll(self, t):
        payload = t.Payload
        words = payload.split()
        cmd = words[0]
        args = words[1:]
        assert cmd == ".POLL"
        if t.Src == self.ID:
            lst = [arg.split(':',2) for arg in args]
            lst = [(node_id, (ip, int(port))) for node_id, ip, port in lst]
            self._update_map(lst)
        else:
            down_ip, down_port = self.downLinkAddress()
            payload += " %s:%s:%d" % (self.ID, down_ip, down_port)
            return payload
            
    def _send_system_poll(self):
        ip, port = self.downLinkAddress()
        self.broadcast(".POLL %s:%s:%d" % (self.ID, ip, port), mutable=True, system=True, fast=False, guaranteed=True)
        
    def _update_map(self, lst):
        #print("Link._update_map:", lst)
        self.Map = lst[:]
        self.DiagonalLink.setDiagonals(self.Map[1:-1])  # remove up node and self
        
    def downConnected(self, node_id, addr):
        #print("EtherLink.downConnected():", node_id, addr)
        self.DownNodeID = node_id
        self.DownNodeAddress = addr
        if self.Delegate is not None and hasattr(self.Delegate, "downConnected"):
            #print("EtherLink.downConnected():", delegate," calling delegate...")
            self.Delegate.downConnected(node_id, addr)
    
    def upConnected(self, node_id, addr):
        #print("EtherLink.downConnected():", node_id, addr)
        self.UpNodeID = node_id
        self.UpNodeAddress = addr
        if self.Delegate is not None and hasattr(self.Delegate, "upConnected"):
            #print("EtherLink.downConnected():", delegate," calling delegate...")
            self.Delegate.upConnected(node_id, addr)
    
    def waitForDownConnection(self, tmo=None):
        return self.DownLink.waitForConnection(tmo)

    def waitForUpConnection(self, tmo=None):
        return self.UpLink.waitForConnection(tmo)
        


        