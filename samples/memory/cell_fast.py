from ring import EtherLink, to_str, to_bytes, EtherLinkDelegate
from pythreader import Primitive, PyThread, Promise, synchronized
import yaml, sys, getopt, time, random, os, json, hashlib


class MemoryCell(EtherLinkDelegate, PyThread):
    
    def __init__(self, link):
        PyThread.__init__(self)
        self.Link = link
        self.Memory = {}
        self.MyID = None
        self.GetPromises = {}            # name -> promise
        
    HashSize = 8
        
    def hash(self, data):
        # returns hash of data as bytes of length equal to the link ID length, or the equivalent integer
        # not supposed to be crypto quality. use for hashing purposes only
        h = hashlib.sha1()
        h.update(to_bytes(data))
        value = h.digest()[-self.HashSize:]
        return int.from_bytes(value, "big")
        
    def parseTransmission(self, t):
        msg = t.str
        words = msg.split(None, 1)
        command, rest = words
        return command, rest
        
    def initialized(self, link_id):
        self.MyID = link_id
        self.MyHash = self.hash(link_id)
        
    def rank(self, cell_hash, name):
        return abs(cell_hash - self.hash(name))

    def i_am_closer(self, name, peer_id):
        # returns True if id_a is closer to name than id_b
        r_a = self.rank(self.MyHash, name)
        r_b = self.rank(self.hash(peer_id), name)
        if r_a < r_b:   return True
        if r_a > r_b:   return False
        return self.MyID < peer_id

    @synchronized
    def get(self, name, timeout=None):
        #print("get()")
        value = self.Memory.get(name)
        if value is not None:
            return value
        promise = self.GetPromises.setdefault(name, Promise())
        #print("sending query")
        self.Link.poll(f"QUERY {name}", guaranteed=True)
        return promise.wait(timeout)

    @synchronized
    def set(self, name, value):
        self.Memory[name] = value
        self.Link.poll(f"UPDATE {self.MyID} {name} {value}")

    def transmissionReturned(self, t):

        command, rest = self.parseTransmission(t)
        if command == "QUERY":
            name = rest
            if t.Src == self.Link.ID:
                # query returned - value not found
                p = self.GetPromises.pop(name, None)
                if p:   
                    p.complete(None)
                    
        elif command == "UPDATE":
            peer_id, name, value = rest.split(None, 2)
            if name in self.Memory:
                value = self.Memory[name]
                self.Link.poll(f"REPLICA {name} {value}", max_edge_hops=2)

    def transmissionReceived(self, t, from_diagonal):
        command, rest = self.parseTransmission(t)

        #print("MemoryCell: received:", t.Src, "->", t.Dst, command, rest)

        if command == "UPDATE":
            peer_id, name, value = rest.split(None, 2)
            if self.i_am_closer(name, peer_id):
                self.Memory[name] = value
                #print("self.Memory:", self.Memory)
                self.Link.poll(f"UPDATE {self.MyID} {name} {value}")
                return False        # stop the original message
            elif name in self.Memory:
                del self.Memory[name]

            p = self.GetPromises.pop(name, None)
            if p:   p.complete(value)
            
        elif command == "VALUE":
            name, value = rest.split(None, 1)
            p = self.GetPromises.pop(name, None)
            if p:   
                p.complete(value)

        elif command == "QUERY":
            name = rest
            #print("Cell: query", name, "in memory:", name in self.Memory)
            #print("     self.Memory:", self.Memory)
            if name in self.Memory:
                value = self.Memory[name]
                #print("sending value...")
                self.Link.send_to(f"VALUE {name} {value}", t.Src, guaranteed=True)
                return False        # stop the query
                
        elif command == "REPLICA":
            name, value = rest.split(None, 1)
            self.Memory[name] = value
            p = self.GetPromises.pop(name, None)
            if p:   
                p.complete(value)

if __name__ == "__main__":
    import getopt, sys, yaml
    opts, args = getopt.getopt(sys.argv[1:], "c:")
    opts = dict(opts)
    config = yaml.load(open(opts["-c"], "r"), Loader=yaml.SafeLoader)
    link = EtherLink(config["ring"])
    cell = MemoryCell(link)
    link.init(cell)
    link.run()
        
    
        
        
        
        
        
        
            
        