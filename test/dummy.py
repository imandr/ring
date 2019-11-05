from link2 import Link
import yaml, sys, getopt, time, random, os
from pythreader import PyThread
from py3 import to_str, to_bytes

opts, args = getopt.getopt(sys.argv[1:], "c:n:")
opts = dict(opts)
name = opts.get("-n", "process#%d" % (os.getpid()))

nodes = yaml.load(open(opts["-c"], "r"), Loader=yaml.SafeLoader)["nodes"]
nodes = [tuple(x) for x in nodes]

#print (nodes)

class MyLink(Link, PyThread):
    
    def __init__(self, nodes, name="unknown"):
        PyThread.__init__(self)
        Link.__init__(self, nodes)
        self.Name = name
        
    def run(self):
        msgid = 0
        while True:
            if random.random() < 0.5:
                self.poll()
            else:
                self.twit()
            time.sleep(random.random()*10)
            
            
    def poll(self):
        print ("Sending poll...")
        self.send("POLL %s" % (self.Name,), mutable=True, send_diagonal=False)
        
    def twit(self):
        print ("Sending twit...")
        self.send("TWIT %s %s" % (self.Name, time.ctime(time.time())))
    
    def processMessage(self, tid, src, dst, msg_bytes):
        #print("processMessage: %s" % (msg_bytes,))
        msg = to_str(msg_bytes)
        if msg.startswith("POLL "):
            words = msg.split(" ",1)
            if src == self.ID:
                nodes = words[1].split(",")
                print ("Online:", ", ".join(nodes))
                return None
            else:
                msg += ",%s" % (self.Name,)
                return to_bytes(msg)
        elif msg.startswith("TWIT "):
            words = msg.split(" ",2)
            print("Twit from %s: %s" % (words[1], words[2]))
        else:
            #print("Unknown mutable message: %s" % (msg,))
            return None
            
    def upConnected(self, nid, addr):
        #print("UpLink connected to:  ", nid, addr)
        pass
            
    def downConnected(self, nid, addr):
        #print("DownLink connected to:", nid, addr)
        pass
            
#link = MyLink(my_index, nodes)
link = MyLink(nodes, name)
#print("link.init()...")
link.init()
link.start()
link.join()
