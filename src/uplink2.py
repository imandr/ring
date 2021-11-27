from pythreader import PyThread, Primitive, synchronized
from .message_stream import MessageStream, StreamTimeout
from socket import *
import random, select, sys, traceback

from .py3 import to_str, to_bytes

class UpLink(PyThread):

    def __init__(self, node, seed_nodes):
        PyThread.__init__(self)
        self.Node = node
        self.SeedNodes = seed_nodes            #[(inx, ip, port), ...]
        self.UpStream = None
        self.UpNodeID = None
        self.UpAddress = None
        self.Shutdown = False
        
    def shutdown(self):
        self.Shutdown = True

    @property
    def upLinkID(self):
        return self.UpNodeID

    def connectStream(self, ip, port):
        try: 
            stream = MessageStream(name="uplink")
            stream.connect((ip, port), 1.0)
            #print(f"UpLink.connectStream: {stream} connected")
        except Exception as e:
            print("UpLink.connectStream: Error connecting to %s:%s: %s" % (ip, port, e))
            stream = None
        else:
            print("connectStream: connected to:", ip, port)
            pass
        return stream

    @synchronized
    def connect_to(self, ip, port):
        #print ("UpLink.connect_to(%s, %d)..." % (ip, port))
        stream = self.connectStream(ip, port)
        if stream is not None:
            #print ("UpLink: connect_to: connected to:", ip, port)
            down_ip, down_port = self.Node.downLinkAddress()
            hello = "HELLO %s %s %s" % (self.Node.ID, down_ip, down_port)
            #print("connect_to: sending", hello, "   and waiting for OK...")
            try:    ok = stream.sendAndRecv(hello)
            except: 
                stream.close()
                return False
            #print("connect_to: response to HELLO:", ok)
            if ok and ok.startswith(b"OK "):
                ok = to_str(ok)
                words = ok.split(None,1)
                self.UpStream = stream
                self.UpAddress = (ip, port)
                self.UpNodeID = words[1]
                self.Node.upConnected(self.UpNodeID, self.UpAddress)
                self.wakeup()
                #print("UpLink.connect: connected to:", ip, port, "  handshake received")
                return True
            else:
                stream.close()
                return False
        else:
            #print("UpLink.connect_to(%s, %d): not connected" % (ip, port))
            return False
    
    def connect(self, connect_first=None):
        #print("UpLink.connect()...")
        up_nodes = [addr for node_id, addr in (self.Node.upNodes() or [])]
        if connect_first is not None:
            up_nodes.insert(0, connect_first)
        for ip, port in up_nodes + self.SeedNodes:
            if self.connect_to(ip, port): 
                break
        else:
            return False
        return True

    def run(self):
        #print ("UpLink.run...")
        
        self.connect()
        connect_to = None
        while not self.Shutdown:
            connected = self.UpStream is not None       # in case it's already connected
            while not connected and not self.Shutdown:
                #print("UpLink: connecting...")
                if not connected:
                    connected = self.connect(connect_to)
                    connect_to = None
                    
                #print("connected to:", self.UpNodeID, '@', self.UpAddress)
                    
            while self.UpStream is not None and not self.Shutdown:
                eof = False
                if self.UpStream.peek(5.0):
                    try:    msg = self.UpStream.recv()
                    except StreamTimeout:
                        #print("UpLink.run: timeout")
                        #self.UpStream.zing()
                        continue
                    if msg is None:
                        #print("UpLink.run: msg is None, UpStream.EOF:", self.UpStream.EOF)
                        eof = True
                    elif msg.startswith(b"RECONNECT "):
                        msg = to_str(msg)
                        cmd, ip, port = msg.split(None, 2)
                        connect_to = (ip, int(port))
                        #print("UpLink: will reconnect to:", ip, port)
                        eof = True
                    else:
                        # ignore ??
                        pass
                if eof:
                    #print("UpLink: eof")
                    self.UpStream.close()
                    self.UpStream = None
                            
    def send(self, transmission):
        #print(f"UpLink.send({transmission})")
        tbytes = transmission.to_bytes()
        
        sent = False
        
        while not sent:
            #print("UpLink.send: waiting for connection...")
            with self:
                while self.UpStream is None:
                    #print("UpLink.waitForConnection(): waiting...")
                    self.sleep(5.0)
                if self.UpStream is not None:
                    try:    sent = self.UpStream.send(tbytes)
                    except:
                        pass
            #print("UpLink.send: sent:", sent)
        #print("UpLink.send(): done")
        
                
        
        
            
            
            
        
           
