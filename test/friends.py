from ring import Link, to_str, to_bytes
import yaml, sys, getopt, time, random, os
from pythreader import PyThread

class MyLink(Link, PyThread):
    
    def __init__(self, nodes, name="unknown"):
        PyThread.__init__(self)
        Link.__init__(self, nodes)
        self.Name = name
        
    def initialized(self):
        #print ("Initialized")
        self.send("HELLO %s" % (self.Name,))
        
    def run(self):
        msgid = 0
        while True:
            if random.random() < 0.5:
                self.poll()
            else:
                self.twit()
            time.sleep(random.random()*10+1.0)
            
    def poll(self):
        print (">> poll")
        self.sendRunner("POLL %s" % (self.Name,))
        
    def runnerReturned(self, t):
        msg = to_str(t.Payload)
        if msg.startswith("POLL "):
            words = msg.split(" ",1)
            nodes = words[1].split(",")
            print ("<<       online:", ", ".join(nodes))

    def twit(self):
        msg = time.ctime(time.time())
        print (">> twit", msg)
        self.send("TWIT %s %s" % (self.Name, time.ctime(time.time())))
        
    
    def processMessage(self, t):
        #print("processMessage: %s" % (msg_bytes,))
        msg = to_str(t.Payload)
        if msg.startswith("POLL "):
            msg += ",%s" % (self.Name,)
            return to_bytes(msg)
        elif msg.startswith("HELLO ") and t.Src != self.ID:
            print("<<       %s joined" % (msg.split(None, 1)[1],))
            print (">> welcome to", msg.split(None, 1)[1])
            self.send("WELCOME %s" % (self.Name,), to=t.Src)
        elif msg.startswith("WELCOME "):
            print("<<       welcome from %s" % (msg.split(None, 1)[1]))
        elif msg.startswith("TWIT "):
            if t.Src != self.ID:
                words = msg.split(" ",2)
                print("<<       twit from %s: %s" % (words[1], words[2]))
        else:
            #print("Unknown mutable message: %s" % (msg,))
            return None
            
opts, args = getopt.getopt(sys.argv[1:], "c:n:")
opts = dict(opts)
name = opts.get("-n", "process#%d" % (os.getpid()))

nodes = yaml.load(open(opts["-c"], "r"), Loader=yaml.SafeLoader)["nodes"]
nodes = [tuple(x) for x in nodes]
#link = MyLink(my_index, nodes)
link = MyLink(nodes, name)
#print("link.init()...")
link.init()
link.start()
link.join()
