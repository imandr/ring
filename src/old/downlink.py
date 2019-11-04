from pythreader import PyThread, Primitive, synchronized
from SockStream import SockStream
from socket import *
import random, select
from transmission import Transmission

from py3 import to_str, to_bytes

class DownLink(PyThread):

    def __init__(self, node, inx, ip, port):
        PyThread.__init__(self)
        self.Node = node
        self.Index = inx
        self.IP = ip
        self.Port = port
        self.ListenSock = None
        self.DownStream = None
        self.DownIndex = None

        self.ListenSock = socket(AF_INET, SOCK_STREAM)
        self.ListenSock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.ListenSock.bind((self.IP, self.Port))
        self.ListenSock.listen(1)

    def run(self):
        print("DownLink started")
        while True:
            lsn_fd = self.ListenSock.fileno()
            lst = [lsn_fd]
            down_fd = None
            if self.DownStream is not None:
                down_fd = self.DownStream.fileno()
                lst.append(down_fd)
            r, w, e = select.select(lst, [], lst, 10.0)
            #print (r,w,e)
            if lsn_fd in r:
                self.acceptDownConnection(self.ListenSock)
            if down_fd in r or down_fd in e:
                self.receiveEdgeTransmission()
                
    def acceptDownConnection(self, lsn_sock):
        #print ("acceptDownConnection()")
        try:    sock, addr = lsn_sock.accept()
        except:
            raise
        
        stream = SockStream(sock)
        msg = stream.recv(tmo = 1.0)
        if msg.startswith("HELLO "):
            cmd, inx = msg.split(" ")
            inx = int(inx)
            if self.DownIndex is None or self.closerDown(inx, self.DownIndex):
                # accept new down connection
                if self.DownStream is not None:
                    self.DownStream.send("RECONNECT")
                    self.DownStream.close()
                self.DownStream = stream
                stream.send("OK")
            else:
                stream.close()
                
    def receiveEdgeTransmission(self):
        self.DownStream.readMore(1024*1024, 10.0)
        while self.DownStream.msgReady():
            msg = self.DownStream.getMsg()
            t = Transmission.from_bytes(msg)
            self.Node.routeTransmission(t, False)
        if self.DownStream.EOF:
            self.DownStream.close()
            self.DownStream = None
