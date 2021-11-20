from pythreader import PyThread, Primitive, synchronized
from socket import *
import random
from .transmission import Transmission

from .py3 import to_str, to_bytes

class DiagonalLink(PyThread):

    def __init__(self, node, address, max_diagonals = 3):
        PyThread.__init__(self)
        self.Node = node
        self.IP, self.Port = address
        self.Sock = None
        self.DiagonalNodes = []
        self.NDiagonals = max_diagonals
        self.Sock = dsock = socket(AF_INET, SOCK_DGRAM)
        dsock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        #print("DiagonalLink: binding to:", (self.IP, self.Port))
        dsock.bind((self.IP, self.Port))
        
    @property
    def address(self):
        return (self.IP, self.Port)
        
    def run(self):
        while True:
            data, addr = self.Sock.recvfrom(65000)
            if data:
                t = Transmission.from_bytes(data)
                self.Node.routeTransmission(t, True)

    #@synchronized
    #def checkDiagonals(self):
    #    if len(self.DiagonalNodes) < self.NDiagonals:
    #        self.Node.pollForDiagonals()
            
    @synchronized
    def addDiagonal(self, node_id, ip, port):
        if (ip, port) not in self.DiagonalNodes:
            self.DiagonalNodes.insert(0, (ip, port))
            self.DiagonalNodes = self.DiagonalNodes[:self.NDiagonals]
        print("DiagLink.addDiagonals: diagonals now:", self.DiagonalNodes)

    @synchronized
    def setDiagonals(self, addresses):
        #self.DiagonalNodes = []
        print("DiagLink.setDiagonals(", addresses, ")")
        if len(addresses) > self.NDiagonals:
            addresses = random.sample(addresses, self.NDiagonals)
        self.DiagonalNodes = addresses
        
    def is_diagonal_link(self, address):
        return address in self.DiagonalNodes

    @synchronized
    def send(self, transmission):
        data = transmission.to_bytes()
        for addr in self.DiagonalNodes:
            #print("DiagonalLink: sending to:", addr)
            self.Sock.sendto(data, addr)
        
