from pythreader import PyThread, Primitive, synchronized
from MessageStream import MessageStream
from socket import *
import random, select
from .transmission import Transmission

from .py3 import to_str, to_bytes

class DownConnection(PyThread):
    
    def __init__(self, mgr, sock):
        PyThread.__init__(self)
        self.Sock = sock
        self.Manager = mgr
        self.NodeID = None
        self.Address = None
        self.Stream = None
        self.Shutdown = False
        
    @synchronized
    def init(self):
        #print("DownConnection.init...")
        stream = MessageStream(self.Sock)
        msg = stream.recv(tmo = 1.0)
        if msg and msg.startswith("HELLO "):
            try:    
                cmd, node_id, ip, port = msg.split(None, 3)
                self.NodeID = node_id
                self.Address = (ip, int(port))
                self.Stream = stream
                self.Manager.downConnected(self)
                stream.send("OK %s" % (self.Manager.nodeID()))
                return True
            except:
                #raise
                stream.close()
                return False
        else:
            stream.close()
            return False
            
    @synchronized
    def sendReconnect(self, addr):
        if self.Stream is not None:
            self.Stream.send("RECONNECT %s %d" % addr)
        self.Shutdown = True
        
    def run(self):
        #print("DownConnection: run...")
        if self.init():
            #print("DownConnection: initialized")
            while not self.Shutdown and self.Stream is not None:
                if self.Stream is not None:
                    #print("DownConnection: recv()...")
                    msg = self.Stream.recv()
                    if msg: 
                        t = Transmission.from_bytes(msg)
                        self.Manager.transmissionReceived(t)
                    else:
                        break
            if self.Stream is not None:
                self.Stream.close()
                self.Stream = None
            self.Manager.downDisconnected(self)
        self.Manager = None
                
class DownLink(PyThread):

    def __init__(self, node, nodes):
        PyThread.__init__(self)
        self.Node = node
        self.ListenSock = None
        self.DownConnection = None
        self.Index, self.ListenSock, self.Address = self.bind(nodes)
        self.ListenSock.listen()
        self.DownNodeAddress = None
        
    def nodeID(self):
        return self.Node.ID
        
    def bind(self, addresses):
        for i, addr in enumerate(addresses):
            try:
                #print("DownLink.bind: trying to bind to:", addr)
                s = socket(AF_INET, SOCK_STREAM)
                s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
                s.bind(addr)
                #print("DownLink.bind: bound to:", addr)
            except Exception as e:
                pass
            else:
                return i, s, s.getsockname()
        else:
            s = socket(AF_INET, SOCK_STREAM)
            s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            s.bind((gethostbyname(gethostname()), 0))
            return None, s, s.getsockname()
            
    @synchronized
    def downConnected(self, connection):
        if self.DownConnection is not None:
            self.DownConnection.sendReconnect(connection.Address)
        self.DownConnection = connection
        self.Node.downConnected(connection.NodeID, connection.Address)

    @synchronized
    def downDisconnected(self, connection):
        self.Node.downDisconnected()
        
    @synchronized
    def waitForConnection(self):
        while self.DownConnection is None:
            self.sleep()
        
    def run(self):
        #print("DownLink started")
        while True:
            #print ("acceptDownConnection()")
            #print("DownLink: accept()...")
            sock, addr = self.ListenSock.accept()
            #print("DownLink: accepted:", addr)
            down_connection = DownConnection(self, sock)
            down_connection.start()
                                
    def transmissionReceived(self, t):
        self.Node.routeTransmission(t, False)
