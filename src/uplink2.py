from pythreader import PyThread, Primitive, synchronized
from MessageStream import MessageStream
from socket import *
import random, select, sys, traceback

from py3 import to_str, to_bytes

class UpLink(PyThread):

    def __init__(self, node, nodes):
        PyThread.__init__(self)
        self.Node = node
        self.UpNodes = nodes            #[(inx, ip, port), ...]
        self.UpStream = None
        self.UpNodeID = None
        self.UpAddress = None

    def init(self):
        self.connect()
        self.start()

    def connectStream(self, ip, port):
        try: stream = MessageStream((ip, port), 1.0)
        except Exception as e:
            print("connectStream: Error connecting to %s %s: %s" % (ip, port, e))
            stream = None
        else:
            print("connectStream: connected to:", ip, port)
        return stream

    @synchronized
    def connect_to(self, ip, port):
        #print ("UpLink.connect_to(%s, %d)..." % (ip, port))
        stream = self.connectStream(ip, port)
        if stream is not None:
            down_ip, down_port = self.Node.downLinkAddress()
            #print("senfing HELLO")
            ok = stream.sendAndRecv("HELLO %s %s %s" % (self.Node.ID, down_ip, down_port))
            #print("response to HELLO:", ok)
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
            return False
    
    @synchronized
    def connect(self):
        #print("UpLink.connect()...")
        for ip, port in self.UpNodes:
            if self.connect_to(ip, port): 
                #print("UpLink.connect: connected")
                break

    def run(self):
        connect_to = None
        while True:

            connected = False
            
            if connect_to is not None: 
                connected = self.connect_to(*connect_to)
                connect_to = None
                
            if not connected:
                self.connect()
                
            self.Node.upConnected(self.UpNodeID, self.UpAddress)
            
            eof = False
            while not eof:
                #print("UpLink.run: readMore...")
                self.UpStream.zing()
                while not eof:
                    msg = self.UpStream.recv()
                    if msg is None:
                        eof = True
                    elif msg.startswith("RECONNECT "):
                        cmd, ip, port = msg.split(None, 2)
                        connect_to = (ip, int(port))
                        eof = True
                if eof or self.UpStream.eof():
                    with self:      # why ?? 
                        self.UpStream = None
                        eof = True
                            
    @synchronized
    def waitForConnection(self):
        while self.UpStream is None:
            self.sleep()

    @synchronized
    def send(self, transmission):

        tbytes = transmission.to_bytes()
        
        sent = False
        
        while not sent:
            while self.UpStream is None:
                #print("UpLink.send(): waiting for connection...")
                self.waitForConnection()
            sent = self.UpStream.send(tbytes)
                
        
        
            
            
            
        
           
