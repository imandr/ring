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
        
    def run(self):
        while True:
            data, addr = self.Sock.recvfrom(65000)
            if data:
                # check source sock address here - later - FIXME
                t = Transmission.from_bytes(data)
                #print("DiagonalLink: received:", t)
                self.Node.routeTransmission(t, True)
                    
    @synchronized
    def setDiagonals(self, nodes):
        #self.DiagonalNodes = []
        self.DiagonalNodes = nodes[:]
        #print ("DiagonalLink: set diagonals to:", nodes)

    @synchronized
    def send(self, transmission):
        data = transmission.to_bytes()
        ndiagonals = min(self.NDiagonals, len(self.DiagonalNodes))
        if ndiagonals:
            diagonals = random.sample(self.DiagonalNodes, ndiagonals)
            for node_id, addr in diagonals:
                #print("DiagonalLink: sending to:", addr)
                self.Sock.sendto(data, addr)
