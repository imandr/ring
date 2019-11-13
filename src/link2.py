from pythreader import PyThread, Primitive, synchronized, Future
from socket import *
import random, time, uuid, yaml
from .transmission import Transmission
from .uplink2 import UpLink
from .downlink3 import DownLink
from .diaglink import DiagonalLink

from .version import Version

class EtherTimeout(Exception):
    pass

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

class EtherLink(PyThread):
    
    Version = Version
    
    def __init__(self, cfg, delegate = None):
        PyThread.__init__(self)
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
        self.Initialized = False
        self.Futures = {}               # tid -> (future, exp_time)
        self.Shutdown = False
        
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
        if self.Delegate is not None and hasattr(self.Delegate, "initialized"):
            self.Delegate.initialized(self.ID)
        self.Initialized = True
            
    def run(self):
        while not self.Shutdown:
            self._purge_futures()
            self._system_poll()
            self.sleep(5.0)
            
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

    def broadcast(self, payload, confirmation=False, timeout = None, fast=True, 
                                guaranteed=False, system=False, mutable=False):
        guaranteed = guaranteed or confirmation
        t = Transmission(self.ID, Transmission.BROADCAST, payload, 
            send_diagonal = fast, cross_to_edge = not guaranteed, mutable=mutable, system=system)
        tid = self._transmit(t)
        ret = t
        if confirmation:
            future = Future(tid)
            self.Futures[tid] = (None if timeout is None else time.time() + timeout, future)
            ret = future
        return ret
        
    def poll(self, payload, confirmation=False, timeout = None, system=False):
        return self.broadcast(payload, fast=False, confirmation=confirmation, guaranteed=True, 
                    mutable=True, system=system, timeout=timeout)

    @synchronized
    def routeTransmission(self, t, from_diagonal):
        
        #print("routeTransmission: from_diagonal=%s %s" % (from_diagonal, t))

        tid = t.TID
        seen, sent_up, sent_diag = self.Seen.get(tid, (False, False, False))
        
        forward = True
        
        if not seen:
            if t.broadcast:
                ret = None
                if t.Src == self.ID and not from_diagonal and not (t.send_diagonal and t.cross_to_edge):
                    # returned brtoadcast, received from diagonal and is guaranteed
                    if tid in self.Futures:
                        _, future = self.Futures.pop(tid)
                        future.complete(t)
                    elif t.system:
                        self._system_broadcast_returned(t)
                    else:
                        if self.Delegate is not None:
                            self.Delegate.messageReturned(t)
                else:
                    # received broadcast
                    if t.system:
                        ret = self._system_message_received(t)
                    else:
                        if self.Delegate is not None:
                            ret = self.Delegate.messageReceived(t)

                    if ret is not None and t.mutable:
                        t.Payload = ret
            else:
                # direct message
                if t.Dst == self.ID:
                    if t.system:
                        self._system_message_received(t)
                    else:
                        if self.Delegate is not None:
                            self.Delegate.messageReceived(t)

        forward = forward and (
                (t.broadcast and t.Src != self.ID) or 
                ((not t.broadcast) and t.Dst != self.ID)
        )
                
        if forward:
            if not sent_up and t.send_edge and (not from_diagonal or t.cross_to_diagonal):
                sent_up = True
                self.UpLink.send(t)
            
            if not sent_diag and t.send_diagonal and (from_diagonal or t.cross_to_diagonal):
                sent_diag = True
                self.DiagonalLink.send(t)
                
        self.Seen.set(tid, (seen, sent_up, sent_diag))
        
    def _system_poll(self):
        ip, port = self.DownLink.Address
        t = self.poll(".POLL %s:%s:%d" % (self.ID, ip, port), system=True, confirmation=True, timeout=5.0) \
            .result()
        if t is not None:
            # poll did return
            payload = t.Payload
            words = payload.split()
            cmd = words[0]
            args = words[1:]
            assert cmd == ".POLL"
            assert t.Src == self.ID
            lst = [arg.split(':',2) for arg in args]
            lst = [(node_id, (ip, int(port))) for node_id, ip, port in lst]
            self.Map = lst[:]
            self.DiagonalLink.setDiagonals(self.Map[1:-1])  # remove immediate up node and self

    def _system_message_received(self, t):
        #print("_system_broadcast_received:",t.Flags,t.Payload)
        payload = t.Payload
        if payload.startswith(".POLL"):
            # system poll
            words = payload.split()
            args = words[1:]
            down_ip, down_port = self.downLinkAddress()
            payload += " %s:%s:%d" % (self.ID, down_ip, down_port)
            return payload
        
    def _system_broadcast_returned(self, t):
        pass        # not used for now
            
    @synchronized
    def _purge_futures(self):
        new_dict = {}
        for tid, (t, future) in self.Futures.items():
            if t is not None and t < time.time():
                # expired
                future.cancel()
            else:
                new_dict[tid] = (t, future)
        self.Futures = new_dict

        
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
        


        