from pythreader import PyThread, Primitive, synchronized
from socket import *
import random

from py3 import to_str, to_bytes

class DiagonalLink(PyThread):

    def __init__(self, node, ip, port, max_diagonals = 3):
        PyThread.__init__(self)
        self.Node = node
        self.Port = port
        self.IP = ip
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
                with self:
                    t = Transmission.from_bytes(data)
                    self.Node.processTransmission(t, True)
                    
    @synchronized
    def setDiagonals(self, nodes):
        self.DiagonalNodes = nodes[:]

    @synchronized
    def send(self, transmission):
        data = transmission.to_bytes()
        ndiagonals = min(self.NDiagonals, len(self.DiagonalNodes))
        if ndiagonals:
            diagonals = random.sample(self.DiagonalNodes, ndiagonals)
            for inx, ip, port in diagonals:
                self.Sock.sendto(data, (ip, port))
