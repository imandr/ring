from pythreader import PyThread, Primitive, synchronized
from fcslib import MessageStream, StreamTimeout
from socket import *
import random, select, sys, traceback

from .py3 import to_str, to_bytes

class UpLink(PyThread):

    def __init__(self, node, nodes):
        PyThread.__init__(self)
        self.Node = node
        self.UpNodes = nodes            #[(inx, ip, port), ...]
        self.UpStream = None
        self.UpNodeID = None
        self.UpAddress = None
        self.Shutdown = False
        
    def shutdown(self):
        self.Shutdown = True

    @property
    def upLinkID(self):
        return self.UpNodeID

    def init(self):
        self.connect()
        self.start()

    def connectStream(self, ip, port):
        try: stream = MessageStream((ip, port), 1.0)
        except Exception as e:
            #print("connectStream: Error connecting to %s %s: %s" % (ip, port, e))
            stream = None
        else:
            #print("connectStream: connected to:", ip, port)
            pass
        return stream

    @synchronized
    def connect_to(self, ip, port):
        #print ("UpLink.connect_to(%s, %d)..." % (ip, port))
        stream = self.connectStream(ip, port)
        if stream is not None:
            print ("UpLink: connect_to: connected to:", ip, port)
            down_ip, down_port = self.Node.downLinkAddress()
            hello = "HELLO %s %s %s" % (self.Node.ID, down_ip, down_port)
            #print("connect_to: sending", hello)
            ok = stream.sendAndRecv(hello)
            print("connect_to: response to HELLO:", ok)
            if ok and ok.startswith("OK "):
                words = ok.split(None,1)
                self.UpStream = stream
                self.UpAddress = (ip, port)
                self.UpNodeID = words[1]
                self.wakeup()
                return True
            else:
                stream.close()
                return False
        else:
            #print("UpLink.connect_to(%s, %d): not connected" % (ip, port))
            return False
    
    def connect(self):
        #print("UpLink.connect()...")
        for ip, port in self.UpNodes:
            if self.connect_to(ip, port): 
                #print("UpLink.connect: connected")
                break
        else:
            return False
        return True

    def run(self):
        
        self.connect()
        connect_to = None
        while not self.Shutdown:
            connected = self.UpStream is not None       # in case it's already connected
            while not connected and not self.Shutdown:
            
                if connect_to is not None: 
                    connected = self.connect_to(*connect_to)
                    connect_to = None
                
                if not connected:
                    connected = self.connect()
                    
            while self.UpStream is not None and not self.Shutdown:
                self.UpStream.zing()
                eof = False
                print("UpLink.run: recv...")
                try:    msg = self.UpStream.recv(1.0)
                except StreamTimeout:
                    print("UpLink.run: timeout")
                    continue
                if msg is None:
                    eof = True
                elif msg.startswith("RECONNECT "):
                    cmd, ip, port = msg.split(None, 2)
                    connect_to = (ip, int(port))
                    eof = True
                else:
                    # ignore ??
                    pass
                if eof:
                    self.UpStream.close()
                    self.UpStream = None
                            
    @synchronized
    def waitForConnection(self, tmo=None):
        while self.UpStream is None:
            #print("UpLink.waitForConnection(): waiting...")
            self.sleep(tmo)
        return self.UpNodeID

    def send(self, transmission):
        #print("UpLink.send()", transmission)

        tbytes = transmission.to_bytes()
        
        sent = False
        
        while not sent:
            self.waitForConnection()
            sent = self.UpStream.send(tbytes)
        #print("UpLink.send(): done")
        
                
        
        
            
            
            
        
           
