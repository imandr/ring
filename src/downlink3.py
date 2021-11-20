from pythreader import PyThread, Primitive, synchronized
from .message_stream import MessageStream, StreamTimeout
from socket import *
import random, select, sys
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
        stream = MessageStream(self.Sock, name="downlink")
        msg = stream.recv(tmo = 1.0)
        #print("DownConnection.init(): received:", msg)
        if msg and msg.startswith(b"HELLO "):
            try:    
                msg = to_str(msg)
                cmd, node_id, ip, port = msg.split(None, 3)
                #print("DownConnection.init(): parsed:", cmd, node_id, ip, port)
                self.NodeID = node_id
                self.Address = (ip, int(port))
                self.Stream = stream
                #print("DownConnection.init(): calling manager.downConnected()...")
                self.Manager.downConnected(self)
                #print("DownConnection.init(): sending OK...")
                stream.send("OK %s" % (self.Manager.nodeID))
                #print("DownConnection.init(): sent OK:")
                #print("DownConnection.init(): initialized")
                self.wakeup()
                #print("DownConnection.init(): returning True")
                return True
            except Exception as e:
                #print("DownConnection.init(): init failed:", e)
                raise
                stream.close()
                return False
        else:
            #print("DownConnection.init(): init failed")
            stream.close()
            return False
    
    def shutdown(self):
        self.Shutdown = True
    
    @synchronized
    def sendReconnect(self, addr):
        if self.Stream is not None:
            #print("DownConnection: sendReconnect to ", addr)
            self.Stream.send("RECONNECT %s %d" % addr)
        self.Shutdown = True
        
    def run(self):
        #print("DownConnection: run...")
        if self.init():
            #print("DownConnection: initialized")
            while not self.Shutdown and self.Stream is not None:
                #print("DownConnection.run: loop")
                if self.Stream is not None:
                    #print(f"DownConnection: {self.Stream}.recv()...")
                    msg = self.Stream.recv()
                    if msg: 
                        t = Transmission.from_bytes(msg)
                        #print("DownConnection: transmission redecived:", t)
                        self.Manager.transmissionReceived(t)
                    else:
                        #print("DownConnection.run: empty msg:", msg)
                        break
            if self.Stream is not None:
                #print("DownConnection.run: closing stream")
                self.Stream.close()
                self.Stream = None
        self.Manager.downDisconnected(self)
        self.Manager = None
                
class DownLink(PyThread):

    def __init__(self, node, seed_nodes):
        PyThread.__init__(self)
        self.Node = node
        self.ListenSock = None
        self.DownConnection = None
        self.Index, self.ListenSock, self.Address = self.bind(seed_nodes)
        self.ListenSock.listen()
        self.DownNodeAddress = None
        self.Shutdown = False
        
    @synchronized
    def shutdown(self):
        self.Shutdown = True
        if self.DownConnection != None:
            self.DownConnection.shutdown()

    def bind(self, addresses):
        for i, addr in enumerate(addresses):
            try:
                #print("DownLink.bind: trying to bind to:", addr)
                s = socket(AF_INET, SOCK_STREAM)
                s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
                s.bind(addr)
                #print("DownLink.bind: bound to:", addr)
            except Exception as e:
                #print("can not bid to addr:", e)
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
        #print("DownLink: down connected:", connection.NodeID)
        if self.DownConnection is not None:
            self.DownConnection.sendReconnect(connection.Address)
        self.DownConnection = connection
        self.Node.downConnected(connection.NodeID, connection.Address)
        self.wakeup()
        
    def downDisconnected(self, connection):
        if connection is self.DownConnection:
            #print("DownLink: down disconnected", connection.NodeID)
            self.Node.downDisconnected(connection.NodeID, connection.Address)

    @synchronized
    def waitForConnection(self, tmo=None):
        while self.DownConnection is None:
            #print("DownLink.waitForConnection(): sleep...")
            self.sleep(tmo)
        #print("DownLink.waitForConnection(): exit")
        return self.downLinkID, self.DownNodeAddress
    
    @property
    def nodeID(self):
        return self.Node.ID
    
    @property
    @synchronized
    def downLinkID(self):
        return None if self.DownConnection is None else self.DownConnection.NodeID
        
    def run(self):
        #print("DownLink started")
        self.ListenSock.settimeout(1.0)
        while not self.Shutdown:
            #print ("acceptDownConnection()")
            #print("DownLink: accept()...")
            try:    sock, addr = self.ListenSock.accept()
            except timeout:
                #print("DownLink: accept() timeout")
                continue
            #print("DownLink: accepted connection from", addr)
            if not self.Shutdown:
                down_connection = DownConnection(self, sock)
                down_connection.start()
        with self:
            self.ListenSock.close()
            self.DownConnection = None
                                
    def transmissionReceived(self, t):
        #print("DownLink: transmissionReceived:", t)
        self.Node.routeTransmission(t, False)
        #print("         return from transmissionReceived:", t.TID)
        
