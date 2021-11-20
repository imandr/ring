from pythreader import PyThread, Primitive, synchronized, Promise
from socket import *
import random, time, uuid, yaml
from .transmission import Transmission
from .uplink2 import UpLink
from .downlink3 import DownLink
from .diaglink import DiagonalLink

from .version import Version
from .py3 import to_str, to_bytes


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
    cfg["seed_nodes"] = [
        (ip, int(port)) 
        for ip, port in 
            [a.split(":", 1) for a in cfg["nodes"]]
    ]
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
        
class EtherLinkMessage(object):
    
    def __init__(self, msg_type, *parts):
        self.MessageType = msg_type
        self.Parts = list(parts)
        
    def append(self, part):
        self.Parts.append(part)
        
    def encode(self):
        words = [self.MessageType]
        for p in self.Parts:
            p = str(p)
            assert " " not in p
            words.append(p)
        return to_bytes(" ".join(words))
    
    @staticmethod
    def decode(data):
        words = data.split(None)
        msg_type, parts = to_str(words[0]), [to_str(p) for p in words[1:]]
        return EtherLinkMessage(msg_type, *parts)
        
    @staticmethod
    def from_transmission(t):
        msg = EtherLinkMessage.decode(t.Payload)
        return msg
        
    def __getitem__(self, i):
        return self.Parts[i]
        
    def __len__(self):
        return self.MessageType
        
    @property
    def type(self):
        return self.MessageType

    @property
    def parts(self):
        return self.Parts

class EtherLink(PyThread):
    
    Version = Version
    
    def __init__(self, cfg, delegate = None):
        PyThread.__init__(self)
        config = read_config(cfg)
        self.Delegate = delegate
        self.ID = uuid.uuid4().hex[:8]
        self.Seen = SeenMemory(10000)               # data: (seen, sent_edge, sent_diagonal)
        seed_nodes = config["seed_nodes"]
        self.DownLink = DownLink(self, seed_nodes)
        inx = self.DownLink.Index
        if inx is not None:
            seed_nodes = seed_nodes[inx+1:] + seed_nodes[:inx+1]
        self.SeedNodes = seed_nodes
        self.UpLink = UpLink(self, seed_nodes)
        self.DiagonalLink = DiagonalLink(self, self.DownLink.Address)
        self.Map = []
        self.Initialized = False
        self.Futures = {}               # tid -> (future, exp_time)
        self.Shutdown = False
        self.DiagonalCheckInterval = 60.0
        self.NextDiagonalCheck = time.time() + self.DiagonalCheckInterval
        self.Debug = True
        
    def __str__(self):
        return f"EtherLink({self.ID} @{self.address})"
        
    def debug(self, *parts):
        if self.Debug:
            print(f"EtherLink {self.ID}:", *parts)
        
    def downLinkAddress(self):
        return self.DownLink.Address

    @property
    def address(self):
        return self.DownLink.Address if self.DownLink is not None else None
        
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
        self.requestDiagonals()
        if self.Delegate is not None and hasattr(self.Delegate, "initialized"):
            self.Delegate.initialized(self.ID)
        self.Initialized = True
        self.debug(f"initialized at {self.address}")
            
    def run(self):
        while not self.Shutdown:
            self._purge_futures()
            if time.time() > self.NextDiagonalCheck:
                self.NextDiagonalCheck += self.DiagonalCheckInterval - 0.5 + random.random()
                #self.DiagonalLink.checkDiagonals()
                self.requestDiagonals()
            self.sleep(15.0)
            
    def shutdown(self):
        self.UpLink.shutdown()
        self.DownLink.shutdown()
        #print("EtherLink.shutdown: waiting for down link...")
        self.DownLink.join()
        #print("EtherLink.shutdown: waiting for up link...")
        self.UpLink.join()
        
    @synchronized
    def _transmit(self, t):
        tid = t.TID
        #print("_transmit: sending...")
        sent_edge, sent_diagonal = False, False
        if t.decrement_hops():
            if t.decrement_edge_hops():
                self.UpLink.send(t)
                sent_edge = True
            #print("_transmit: sent up: %s" % (t,))
            if t.send_diagonal and t.decrement_diagonal_hops():
                self.DiagonalLink.send(t)
                sent_diagonal = True
        #print("_transmit:", sent_edge, sent_diagonal)
        self.Seen.set(tid, (False, sent_edge, sent_diagonal))
        return tid

    def send(self, payload, to, system=False, send_diagonal=True):
        assert to != Transmission.BROADCAST, "Use EtherLink.broadcast() to send broadcast messages"
        t = Transmission(self.ID, to, payload, send_diagonal = send_diagonal, system=system)
        return self._transmit(t)

    def broadcast(self, payload, confirmation=False, timeout = None, fast=True, 
                                max_hops = None, max_edge_hops = None, max_diagonal_hops = None,
                                guaranteed=False, system=False, mutable=False):
        guaranteed = guaranteed or confirmation
        t = Transmission(self.ID, Transmission.BROADCAST, payload, 
            max_hops = max_hops, max_edge_hops = max_edge_hops, max_diagonal_hops = max_diagonal_hops,
            send_diagonal = fast, cross_to_edge = not guaranteed, mutable=mutable, system=system)
        tid = self._transmit(t)
        ret = t
        if confirmation:
            future = Promise(tid)
            self.Futures[tid] = (None if timeout is None else time.time() + timeout, future)
            ret = future
        return ret
        
    def poll(self, payload, confirmation=False, timeout = None, system=False):
        return self.broadcast(payload, fast=False, confirmation=confirmation, guaranteed=True, 
                    mutable=True, system=system, timeout=timeout)
                    
    def message_received(self, t):
        if t.system:
            ret = self._system_message_received(t)
        else:
            if self.Delegate is not None:
                ret = self.Delegate.messageReceived(t)
                
    def broadcast_received(self, t):
        if t.system:
            self._system_message_received(t)
        elif self.Delegate is not None:
            self.Delegate.messageReceived(t)
        
    def mutable_broadcast_received(self, t):
        if t.system:
            payload = self._system_message_received(t)
        elif self.Delegate is not None:
            payload = self.Delegate.messageReceived(t)
        else:
            payload = None
        return payload
        
    def broadcast_returned(self, t):
        tid = t.TID
        if tid in self.Futures:
            _, future = self.Futures.pop(tid)
            future.complete(t)
        elif t.system:
            self._system_broadcast_returned(t)
        elif self.Delegate is not None:
            self.Delegate.messageReturned(t)

    @synchronized
    def routeTransmission(self, t, from_diagonal):
        print(f"{self}: route transmission:", t)
        
        tid = t.TID
        seen, sent_edge, sent_diag = self.Seen.get(tid, (False, False, False))
        #print("EtherLink.routeTransmission: link:", id(self), "   tid:", tid, "   seen:", seen)
        
        new_payload = None
        
        if not seen:
            # this message is new. Process it
            if t.broadcast:
                if t.Src == self.ID and from_diagonal:
                    self.broadcast_returned(t)
                elif t.mutable and not from_diagonal:
                    new_payload = self.mutable_broadcast_received(t)
                else:
                    self.broadcast_received(t)                    
            elif t.Dst == self.ID:
                self.message_received(t)
            seen = True
            
        forward = (
                    t.broadcast and t.Src != self.ID 
                    or 
                    (not t.broadcast) and t.Dst != self.ID
                )
        
        if forward and t.decrement_hops():

            if new_payload is not None:
                t.set_payload(new_payload)

            if not sent_edge and t.send_edge and (not from_diagonal or t.cross_to_diagonal):
                if t.decrement_edge_hops():
                    sent_edge = True
                    self.UpLink.send(t)
        
            if not sent_diag and t.send_diagonal and (from_diagonal or t.cross_to_diagonal):
                if t.decrement_diagonal_hops():
                    sent_diag = True
                    self.DiagonalLink.send(t)
            
        self.Seen.set(tid, (True, sent_edge, sent_diag))
        
    def pollForDiagonals(self):
        self.sendConnectivityPoll()

    def requestDiagonals(self):
        print(f"{self}: requesting diagonals")
        diag_ip, diag_port = self.DiagonalLink.address
        self.broadcast("DREQ %s %d" % (diag_ip, diag_port), system=True, max_diagonal_hops=5, max_edge_hops=5)

    def sendConnectivityPoll(self):
        ip, port = self.address
        self.poll("POLL %s:%s:%d" % (self.ID, ip, port), system=True)

    def _system_message_received(self, t):
        print(f"{self}: _system_message_received:",t)
        payload = t.Payload
        msg = EtherLinkMessage.from_transmission(t)
        print("    msg.type=", msg.type)
        if msg.type == "POLL":
            # system poll
            lst = msg.parts
            ip, port = self.address
            msg.append("%s:%s:%d" % (self.ID, ip, port))
            return msg.encode()
        elif msg.type == "DCON":
            node_id, ip, port = msg.parts
            addr = (ip, int(port))
            if self.DiagonalLink.is_diagonal_link(addr):
                self.pollForDiagonals()
        elif msg.type == "DREQ":
            if t.Src != self.DownLink.downLinkID and t.Src != self.UpLink.upLinkID:
                print(f"{self}: received diagonal request from {t.Src}. my down link:{self.DownLink.downLinkID}, up link:{self.UpLink.upLinkID}")
                if random.random() < 1.0:
                    ip, port = msg.parts
                    port = int(port)
                    print("       replying to:", ip, port)
                    response = EtherLinkMessage("DREP", *self.DiagonalLink.address).encode()
                    self.send(response, t.Src, system=True)
        elif msg.type == "DREP":
            ip, port = msg.parts
            src = t.Src
            print(f"{self}: received diagonal reply from {src} with address: {ip}:{port}")
            self.DiagonalLink.addDiagonal(src, ip, int(port))
        
    def _system_broadcast_returned(self, t):
        assert t.Src == self.ID
        payload = t.Payload
        msg = EtherLinkMessage.from_transmission(t)
        if msg.type == "POLL":
            lst = [arg.split(':',2) for arg in msg.parts]
            lst = [(node_id, (ip, int(port))) for node_id, ip, port in lst]
            if self.Debug:
                print("Connectivity map:")
                for node_id, (ip, port) in lst:
                    print(f"  {node_id}@{ip}:{port}", "(self)" if node_id == self.ID else "")
            self.Map = lst
            self.DiagonalLink.setDiagonals([addr for node_id, addr in self.Map[1:-1]])  # remove immediate up node and self
            
    def upNodes(self):
        return self.Map[1:]     # last known up chain
            
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
            
    def downDisconnected(self, node_id, addr):
        ip, port = addr
        payload = "DCON %s %s %d" % (node_id, ip, port)
        self.broadcast(payload, system=True)

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
        


        