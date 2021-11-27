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

class SystemMessage(object):
    
    def __init__(self, msg_type, *parts):
        self.MessageType = msg_type
        self.Parts = list(parts)
        self.Src = self.Dst = None
        
    def __str__(self):
        return f"SystemMessage({self.MessageType}, %s)" % (",".join(self.Parts),)
        
    def append(self, part):
        self.Parts.append(part)
        
    def encode(self, encoding="utf-8"):
        words = [self.MessageType]
        for p in self.Parts:
            p = str(p)
            assert " " not in p
            words.append(p)
        return (" ".join(words)).encode(encoding)
    
    @staticmethod
    def decode(payload, encoding="utf-8"):
        if isinstance(payload, bytes):
            payload = payload.decode(encoding)
        words = payload.split()
        msg_type, parts = words[0], words[1:]
        return SystemMessage(msg_type, *parts)
        
    @staticmethod
    def from_transmission(t):
        msg = SystemMessage.decode(t.payload_str)
        msg.Src = t.Src
        msg.Dst = t.Dst
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
            lst = sorted(self.Memory.items(), key=lambda x: x[1][0], reverse=True)
            self.Memory = dict(lst[:self.LowWater])
            
class EtherLinkDelegate(object):    # virtual base class for delegates
    
    def transmissionReturned(self, t):
        # src = self.ID and not from diagonal
        pass
        
    def transmissionReceived(self, t, from_diagonal):
        pass
        
    def initialized(self, node_id):
        pass
        
    def downConnected(self, node_id, addr):
        pass
        
    def upConnected(self, node_id, addr):
        pass
        
class EtherLink(PyThread):
    
    Version = Version
    ID_Length = 4           # in bytes
    
    def __init__(self, config):
        PyThread.__init__(self)
        config = self.read_config(config)
        
        uid = uuid.uuid4()
        self.ID_bytes = uid.bytes[:self.ID_Length]
        self.ID = self.ID_bytes.hex()
        
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
        return f"EtherLink({self.ID} @%s:%s)" % self.address
        
    def read_config(self, config):
        cfg = {}
        cfg["seed_nodes"] = [
            (ip, int(port)) 
            for ip, port in 
                [a.split(":", 1) for a in config["seed_nodes"]]
        ]
        #print("read_config: nodes:", cfg["nodes"])
        return cfg

    def debug(self, *parts):
        if self.Debug:
            print(f"EtherLink:", *parts)
        
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
        self.debug(f"{self} initialized")
            
    def run(self):
        self.sendConnectivityPoll()
        while not self.Shutdown:
            self._purge_futures()
            if False and time.time() > self.NextDiagonalCheck:
                self.NextDiagonalCheck += self.DiagonalCheckInterval*(random.random() + 0.5)
                #self.DiagonalLink.checkDiagonals()
                self.sendConnectivityPoll()
                self.requestDiagonals()
            self.sleep(15.0)
            
    def shutdown(self):
        self.UpLink.shutdown()
        self.DownLink.shutdown()
        #print("EtherLink.shutdown: waiting for down link...")
        self.DownLink.join()
        #print("EtherLink.shutdown: waiting for up link...")
        self.UpLink.join()
        
    def send_to(self, payload, to, **args):
        assert to != Transmission.BROADCAST, "Use EtherLink.broadcast() to send broadcast messages"
        t = Transmission(self.ID, to, payload, **args)
        return self.forward(t, False, False, False)

    def broadcast(self, payload, wait=False, timeout = None, guaranteed=False, **args):
        t = Transmission.broadcast(self.ID, payload, guaranteed = guaranteed, **args)
        ret = t
        if wait:
            future = Promise(tid)
            self.Futures[tid] = (None if timeout is None else time.time() + timeout, future)
            ret = future
        self.forward(t, False, False, False)
        return ret
        
    def poll(self, payload, guaranteed=True, **args):
        return self.broadcast(payload, 
            guaranteed=True,    # override
            max_diagonal_hops=0, **args)
                    
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
        
    def forward(self, t, sent_edge, sent_diagonal, from_diagonal):
        
        # if only 1 hop left, choose the edge over diagonal
        
        if not sent_edge:
            if from_diagonal and t.is_guaranteed:
                pass    # do not send
            elif t.decrement_edge_hops():
                self.UpLink.send(t)
                print("Link: >>> send (e):", t)
                sent_edge = True

        if not sent_diagonal:
            if (not t.is_guaranteed) and t.decrement_diagonal_hops():
                self.DiagonalLink.send(t)
                print("Link: >>> send (d):", t)
                sent_diagonal = True
            
        return sent_edge, sent_diagonal
            
    #
    # message processing
    #

    def receive(self, t, from_diagonal):
        new_payload = None
        stop = False
        tid = t.TID
        if tid in self.Futures:
            _, f = self.Futures.pop(tid)
            #print("Link.receive: completing promise:", f)
            if t.system:
                f.complete(SystemMessage.from_transmision(t))
            else:
                f.complete((t.digest, from_diagonal))
        else:
            if t.is_system:
                msg = SystemMessage.from_transmission(t)
                ret = self.system_message_received(msg, from_diagonal)
                if ret is not None:
                    new_message, stop = ret
                    if not stop and new_message is not None and t.is_mutable:
                        new_payload = new_message.encode()
            elif self.Delegate is not None:
                if t.Src == self.ID and not from_diagonal:
                    self.Delegate.transmissionReturned(t.digest)
                else:
                    ret = self.Delegate.transmissionReceived(t.digest, from_diagonal)
                    if ret is False:
                        stop = True
                    elif ret is None:
                        pass        # pass through
                    elif t.is_mutable:
                        assert isinstance(ret, (str, bytes))
                        new_payload = ret
                    else:
                        pass        # pass through

        return new_payload, stop
        
    #
    # system message processing
    #
        
    def requestDiagonals(self):
        #print(f"{self}: requesting diagonals")
        diag_ip, diag_port = self.DiagonalLink.address
        msg = SystemMessage("DREQ", diag_ip, diag_port)
        self.broadcast(msg.encode(), system=True, max_diagonal_hops=1, max_edge_hops=3)

    def sendConnectivityPoll(self):
        ip, port = self.address
        msg = SystemMessage("POLL", "%s:%s:%d" % (self.ID, ip, port))
        self.poll(msg.encode(), system=True, mutable=True)

    def system_message_received(self, msg, from_diagonal):
        #print(f"{self}: _system_message_received:", src, "->", dst, ":", msg)
        src = msg.Src
        dst = msg.Dst
        if msg.type == "POLL":
            lst = [arg.split(':',2) for arg in msg.parts]
            lst = [(node_id, (ip, int(port))) for node_id, ip, port in lst]
            if src == self.ID:
                self.Map = lst
                if self.Debug:
                    print("Connectivity map:")
                    for node_id, (ip, port) in lst:
                        print(f"  {node_id}@{ip}:{port}", "(self)" if node_id == self.ID else "")
            else:
                lst = msg.parts
                ip, port = self.address
                msg.append("%s:%s:%d" % (self.ID, ip, port))
                #print("_system_message_received: returning:", msg)
                return msg, False
        elif msg.type == "DCON" and src != self.ID:
            node_id, ip, port = msg.parts
            addr = (ip, int(port))
            if self.DiagonalLink.is_diagonal_link(addr):
                self.requestDiagonals()
        elif msg.type == "DREQ" and src != self.ID:
            if src != self.DownLink.downLinkID and src != self.UpLink.upLinkID:
                #print(f"{self}: received diagonal request from {src}. my down link:{self.DownLink.downLinkID}, up link:{self.UpLink.upLinkID}")
                if random.random() < 1.0:
                    response = SystemMessage("DREP", *self.DiagonalLink.address)
                    self.send_to(response.encode(), src, system=True)
        elif msg.type == "DREP" and src == self.ID:
            ip, port = msg.parts
            #print(f"{self}: received diagonal reply from {src} with address: {ip}:{port}")
            self.DiagonalLink.addDiagonal(src, ip, int(port))
        else:
            # ignore
            pass
        
    #
    # ring protocol callbacks and methods
    #
    @synchronized
    def transmissionReceived(self, t, from_diagonal):
        print("Link: <<< recv (%s):" % ('d' if from_diagonal else 'e',), t)
        tid = t.TID
        received, sent_edge, sent_diag = self.Seen.get(tid, (False, False, False))
        stop = False
        #print("    received, sent_edge, sent_diag:", received, sent_edge, sent_diag)

        if not received:
            
            receive = False
            if t.Dst == self.ID:    receive = True
            elif t.is_broadcast:
                if t.Src == self.ID:
                    receive = not from_diagonal or not t.is_guaranteed
                else:
                    receive = True

            #print("    receive =", receive)
            if receive:
                print("Link: [consume]")
                new_payload, stop = self.receive(t, from_diagonal)
                if t.mutable and new_payload is not None:
                    #print("updating payload")
                    t.payload = new_payload
                received = True

        if t.Src != self.ID:
            if not stop and not (sent_edge and sent_diag):
                sent_edge, sent_diag = self.forward(t, sent_edge, sent_diag, from_diagonal)
        
        self.Seen.set(tid, (received, sent_edge, sent_diag))  # no need to update Seen for own messages
            
    def downConnected(self, node_id, addr):
        #print("EtherLink.downConnected():", node_id, addr)
        self.DownNodeID = node_id
        self.DownNodeAddress = addr
        if self.Delegate is not None and hasattr(self.Delegate, "downConnected"):
            #print("EtherLink.downConnected():", delegate," calling delegate...")
            self.Delegate.downConnected(node_id, addr)
            
    def downDisconnected(self, node_id, addr):
        ip, port = addr
        msg = SystemMessage("DCON", node_id, ip, port)
        self.broadcast(msg.encode(), system=True)

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
        
    def upNodes(self):
        return self.Map[1:]     # last known up chain
            


        