from ring import EtherLink, to_str, to_bytes, EtherLinkDelegate
from pythreader import Primitive
import yaml, sys, getopt, time, random, os, json
from pythreader import PyThread

Usage = """
Usage: python pubsub.py -c <config file> subscribe <topic> ...
       python pubsub.py -c <config file> publish <topic> ...
"""


class PubSubAgent(EtherLinkDelegate):
    
    def __init__(self, ether, topics=[]):
        self.Ether = ether
        self.SubscribedTopics = set(topics)
        
    def subscribe(self, topic):
        self.Topics.add(topic)
        
    def unsubscribe(self, topic):
        self.Topics.remove(topic)
        
    def publish(self, topic, article):
        #print("broadcast")
        self.Ether.broadcast("ARTICLE %s %s" % (topic, article))
        
    def transmissionReceived(self, t, from_diagonal):
        msg = t.str
        cmd, rest = msg.split(None, 1)
        #print("PubSubAgent.messageReceived:", msg)
        if cmd == "ARTICLE":
            topic, article = rest.split(None, 1)
            if topic in self.SubscribedTopics:
                self.published(topic, article)
                
    def shutdown(self):
        self.Ether.shutdown()
                
class Publisher(PubSubAgent, PyThread):
    
    def __init__(self, ether, topics):
        PyThread.__init__(self)
        PubSubAgent.__init__(self, ether)
        self.Topics = topics
        self.Words = None
        try:
            with open("/usr/share/dict/words", "r") as words:
                self.Words = [w.strip() for w in words]
        except:
            pass            
    
    def run(self):
        self.Ether.init(self)
        self.Ether.start()
        while True:
            topic = random.choice(self.Topics)
            if self.Words:
                article = " ".join(random.sample(self.Words, 3))
            else:
                article = time.ctime(time.time())
            if True:
                print(f"publish: [{topic}] {article}")
                self.publish(topic, article)
            time.sleep(1.0+random.random()*10.0)

class Subscriber(PubSubAgent, PyThread):
    
    def __init__(self, ether, topics):
        PyThread.__init__(self)
        PubSubAgent.__init__(self, ether, topics)
    
    def published(self, topic, article):
        print(f"received: [{topic}] {article}")
        
    def run(self):
        self.Ether.init(self)
        self.Ether.start()
                


if __name__ == '__main__':
    import sys, getopt, random, yaml
        
    opts, args = getopt.getopt(sys.argv[1:], "c:")
    opts = dict(opts)
    if len(args) < 2 or not "-c" in opts:
        print(Usage)
        os.exit(1)
    config = yaml.load(open(opts["-c"], "r"), Loader=yaml.SafeLoader)
    ether = EtherLink(config["ring"])

    mode = args[0]
    if mode == "publish":
        agent = Publisher(ether, args[1:])
        agent.start()
        agent.join()
    elif mode == "subscribe":
        agent = Subscriber(ether, args[1:])
        agent.start()
        agent.join()
    else:
        print(Usage)
        
        
    

                        
        