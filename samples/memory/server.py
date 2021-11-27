import sys, getopt, yaml
from webpie import WPApp, WPHandler, HTTPServer

from cell import MemoryCell
from ring import EtherLink

class Handler(WPHandler):
    
    def get(self, request, name):
        cell = self.App.Cell
        value = cell.get(name)
        return value if value is not None else 404
        
    def set(self, request, name, value=None):
        cell = self.App.Cell
        value = cell.set(name, value)
        return 200

class App(WPApp):
    
    def __init__(self, cell):
        WPApp.__init__(self, Handler)
        self.Cell = cell

opts, args = getopt.getopt(sys.argv[1:], "c:")
opts = dict(opts)
config = yaml.load(open(opts["-c"], "r"), Loader=yaml.SafeLoader)
port = int(config["server"]["port"])
link = EtherLink(config["ring"])
cell = MemoryCell(link)
link.init(cell)
link.start()

server = HTTPServer(port, App(cell))
server.start()
server.join()

