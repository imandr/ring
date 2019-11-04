from pythreader import PyThread, synchronized
import time, random

class Link(PyThread):
    
    def __init__(self, id, index, core):
        PyThread.__init__(self)
        self.Core = core
        self.id = id
        self.index = index
        self.up = None
        self.down = None
        self.Shutdown = False
        self.connect_to = None
        
    def __str__(self):
        return "<%s>" % (self.id,)
        
    __repr__ = __str__

    @synchronized
    def reconnect_up(self, to):
        print ("%s: reconnect to: %s" % (self, to))
        self.connect_to = to
        self.up = None
        self.wakeup()

    @synchronized
    def connect_up(self, to=None):
        print("%s: connect_up:" % (self,), to)
        to = to or self.connect_to
        self.connect_to = None
        lst = self.Core[:]
        if self.index is not None:
            lst = self.Core[self.index+1:] + self.Core[:self.index+1]
        if to is not None:
            lst = [to] + lst
        print("%s: lst: %s" % (self, lst))
        for l in lst:
            if not l.Shutdown:
                self.up = l
                l.connected_down(self)
                self.wakeup()
                break
        print ("%s: up connected to: %s" % (self, self.up))
        
    @synchronized
    def connected_down(self, down):
        print("%s: connected down from: %s" % (self, down))
        d = self.down
        self.down = down
        if d is not None:
            d.reconnect_up(down)
        self.wakeup()
        
    @synchronized
    def up_disconnected(self):
        print("%s: up disconnected" % (self,))
        self.up = None
        self.wakeup()
        
    @synchronized
    def down_disconnected(self):
        print("%s: down disconnected" % (self,))
        self.down = None
        # disconnect up here ?
        self.wakeup()
        
    def run(self):
        print("%s: run: core:" % (self,), id(self.Core), self.Core)
        while not self.Shutdown:
            #time.sleep(random.random()*0.1)
            if self.up is None:
                self.connect_up()
            while self.up is not None:
                self.sleep()
                
    def kill(self):
        print("%s: terminate" % (self,))
        self.Shutdown = True
        if self.down is not None:   self.down.up_disconnected()
        if self.up is not None:     self.up.down_disconnected()
        self.up = self.down = None
        self.wakeup()
            
        
if __name__ == '__main__':
    import getopt, sys
    opts, args = getopt.getopt(sys.argv[1:], "c:n:")
    opts = dict(opts)
    ncore = int(opts["-c"])
    nout = int(opts["-n"])
    
    i = 0
    core = []
    outsiders = []
    for i in range(ncore+nout):
        if i < ncore:
            core.append(Link(i, i, core))
        else:
            outsiders.append(Link(i, None, core))
        i += 1
    
    links = core + outsiders
    
    #random.shuffle(links)
    
    for l in links:
        l.start()
        time.sleep(0.01)
        
    time.sleep(3)
    for l in links:
        if not l.Shutdown:
            print ("%s -> %s" % (l, l.up))
        
    l = random.choice(links)
    print("killing:", l)
    l.kill()
    
    
    #for l in core[1:]:
    #    print("killing:", l)
    #    l.kill()
    
    time.sleep(3)
    for l in links:
        if not l.Shutdown:
            print ("%s -> %s" % (l, l.up))
    
    
    
    
    
