from pythreader import PyThread, Primitive, synchronized
from SockStream import SockStream
from socket import *
import random, select

from py3 import to_str, to_bytes

class UpLink(Primitive):

    def __init__(self, node, inx, nodes):
        PyThread.__init__(self)
        self.Node = node
        self.Index = inx
        self.UpNodes = nodes            #[(inx, ip, port), ...]
        self.UpStream = None
        self.UpIndex = None
        self.connectUp()

    def connectStream(self, ip, port):
        #print ("connectStream", ip, port)
        sock = socket(AF_INET, SOCK_STREAM)
        sock.settimeout(1.0)
        try:    
            #print("connecting...")
            sock.connect((ip, port))
            #print("connected")
        except:
            sock.close()
            return None
        stream = SockStream(sock)
        #print ("sendAndRecv...")
        reply = stream.sendAndRecv("HELLO %d" % (self.Index,))
        #print("reply:", reply)
        if reply != "OK":
            return None
        sock.settimeout(None)
        #print("return from connectStream()")
        return stream

    @synchronized
    def connectUp(self):
        print ("connectUp()")
        j = None
        while self.UpStream is None:
            for j, (inx, ip, port) in enumerate(self.UpNodes):
                stream = self.connectStream(ip, port)
                if stream is not None:  
                    self.UpStream = stream
                    self.UpInx = inx
                    break
            else:
                self.sleep(1.0)
        self.Node.uplinkConnectedJ(j)

    def run(self):
        while True:
            self.connectUp()
            self.wakeup()
            eof = False
            while not eof:
                self.UpStream.readMore(tmo=10)
                while self.UpStream.msgReady():
                    # ??? uplink should not send anything
                    self.UpStream.getMsg()
                if self.UpStream.eof():
                    self.UpStream.close()
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
            if self.UpStream is None:
                self.connectUp()
            if self.UpStream is not None:
                nsent = self.UpStream.send(tbytes)
                if nsent < len(tbytes):
                    self.UpStream.Sock.close()
                    self.UpStream = None
                else:
                    sent = True
                
        
        
            
            
            
        
           
