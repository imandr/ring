from ring import EtherLink, to_str, to_bytes, EtherLinkDelegate
import yaml, sys, getopt, time, random, os
from pythreader import PyThread

class Friends(PyThread, EtherLinkDelegate):
    
    def __init__(self, ether, name="unknown"):
        PyThread.__init__(self)
        self.Name = name
        self.Ether = ether
        
    def initialized(self, node_id):
        #print ("Initialized")
        self.Ether.broadcast("HELLO %s" % (self.Name,))
        
    def run(self):
        while True:
            if random.random() < 0.5:
                self.poll()
            else:
                self.twit()
            time.sleep(random.random()*10+1.0)
            
    def poll(self):
        print (">> poll")
        t = self.Ether.poll("POLL %s" % (self.Name,), confirmation=True, timeout=1.0).result()
        if t is not None:
            msg = to_str(t.Payload)
            words = msg.split(" ",1)
            nodes = words[1].split(",")
            print ("<<       online:", ", ".join(nodes))
        else:
            print ("<<       [poll lost]")
            
    def twit(self):
        msg = time.ctime(time.time())
        print (">> twit", msg)
        self.Ether.broadcast("TWIT %s %s" % (self.Name, time.ctime(time.time())))
    
    def messageReceived(self, t):
        #print("processMessage: %s" % (msg_bytes,))
        msg = to_str(t.Payload)
        if msg.startswith("POLL "):
            msg += ",%s" % (self.Name,)
            return to_bytes(msg)
        elif msg.startswith("HELLO ") and t.Src != self.Ether.ID:
            print("<<       %s joined" % (msg.split(None, 1)[1],))
            print (">> welcome to", msg.split(None, 1)[1])
            self.Ether.send("WELCOME %s" % (self.Name,), t.Src)
        elif msg.startswith("WELCOME "):
            print("<<       welcome from %s" % (msg.split(None, 1)[1]))
        elif msg.startswith("TWIT "):
            if t.Src != self.Ether.ID:
                words = msg.split(" ",2)
                print("<<       twit from %s: %s" % (words[1], words[2]))
        else:
            #print("Unknown mutable message: %s" % (msg,))
            return None
            
opts, args = getopt.getopt(sys.argv[1:], "c:n:")
opts = dict(opts)
name = opts.get("-n", "process#%d" % (os.getpid()))
cfg = opts["-c"]

link = EtherLink(cfg)
network = Friends(link, name)
link.init(network)
link.start()
network.start()
network.join()
