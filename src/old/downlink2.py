from pythreader import PyThread, Primitive, synchronized
from MessageStream import MessageStream
from socket import *
import random, select
from transmission import Transmission

from py3 import to_str, to_bytes

class DownLink(PyThread):

    def __init__(self, node, nodes):
        PyThread.__init__(self)
        self.Node = node
        self.ListenSock = None
        self.DownStream = None
        self.Index, self.ListenSock, self.Address = self.bind(nodes)
        #print("DownLink bound to:", self.Address)
        self.ListenSock.listen()
        self.DownNodeAddress = None
        
    def bind(self, addresses):
        for i, addr in enumerate(addresses):
            try:
                #print("DownLink.bind: trying to bind to:", addr)
                s = socket(AF_INET, SOCK_STREAM)
                s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
                s.bind(addr)
            except Exception as e:
                pass
            else:
                return i, s, s.getsockname()
        else:
            s = socket(AF_INET, SOCK_STREAM)
            s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            s.bind((gethostbyname(gethostname()), 0))
            return None, s, s.getsockname()

    def run(self):
        #print("DownLink started")
        while True:
            lsn_fd = self.ListenSock.fileno()
            lst = [lsn_fd]
            down_fd = None
            if self.DownStream is not None:
                down_fd = self.DownStream.fileno()
                lst.append(down_fd)
            print("DownLink.run: selecting...", lst)
            r, w, e = select.select(lst, [], lst, 10.0)
            print("DownLink.run: selected", r, w, e)
            if lsn_fd in r:
                self.acceptDownConnection()
            if down_fd in r or down_fd in e:
                self.readEdge()
                
    def acceptDownConnection(self):
        #print ("acceptDownConnection()")
        sock, addr = self.ListenSock.accept()
        stream = MessageStream(sock)
        down_node_addr = None
        msg = stream.recv(tmo = 1.0)
        #print("DownLink: acceptDownConnection: HELLO message:", msg)
        if msg.startswith("HELLO "):
            try:    
                cmd, down_node_id, ip, port = msg.split(None, 3)
                down_node_addr = (ip, int(port))
                self.DownNodeID = down_node_id
                self.DownNodeAddress = down_node_addr
                stream.send("OK %s" % (self.Node.ID))
                self.Node.downConnected(self.DownNodeID, self.DownNodeAddress)
            except:
                stream.close()
                stream = None
        else:
            stream.close()
            stream = None
        
        if stream is not None:
            with self:
                if self.DownStream is not None:
                    #print("DownLink.acceptDownConnection: disconnecting existing down stream...")
                    #print("          sending RECONNECT...")
                    self.DownStream.send("RECONNECT %s %d" % down_node_addr)
                    self.DownStream.close()
                    #print("          old down stream closed")
                self.DownStream = stream
                self.DownNodeAddress = down_node_addr
                
    def readEdge(self):
        msg = self.DownStream.recv()
        if msg is None:
            self.DownStream.close()
            self.DownStream = None
        else:
            t = Transmission.from_bytes(msg)
            self.Node.routeTransmission(t, False)
