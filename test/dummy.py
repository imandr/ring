from link import Link
import yaml, sys, getopt, time, random
from pythreader import PyThread
from py3 import to_str

opts, args = getopt.getopt(sys.argv[1:], "n:i:")
opts = dict(opts)
my_index = int(opts["-i"])

nodes = yaml.load(open(opts["-n"], "r"))["nodes"]

print (nodes)

class MyLink(Link, PyThread):
    
    def __init__(self, my_index, nodes):
        PyThread.__init__(self)
        Link.__init__(self, my_index, nodes)
        
    def run(self):
        msgid = 0
        while True:
            time.sleep(random.random()*5)
            msg = "msg %d %d" % (self.Index, msgid)
            msgid += 1
            print ("Sending: [%s]" % (msg,))
            self.send(msg)
        
            
    def messageReceived(self, src, dst, msg_bytes):
        print ("Received from %s to %s: [%s]" % (src, dst, to_str(msg_bytes)))
        
link = MyLink(my_index, nodes)
link.init()
link.start()
link.join()