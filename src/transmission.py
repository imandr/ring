import time

from py3 import to_str, to_bytes


class Transmission(object):

    BROADCAST = -1      # dest -> BROADCAST
    FLAG_EDGE = 'e'     # can be sent over the edge link
    FLAG_CROSS = 'x'    # can cross between edge and diagonal
    FLAG_DIAGONAL = 'd' # can be sent over diagonal link

    def __init__(self, source_index, dest_index, payload, send_diagonal=True, cross_diagional=True):
        t = time.time()
        self.TID = "%d.%d.%d" % (source_index, int(t), int((t-int(t))*1000000))
        self.Src = source_index
        self.Dst = self.BROADCAST if dest_index is None else dest_index 
        self.Flags = 'e' \
            + self.FLAG_CROSS if cross_diagional else '' \
            + self.FLAG_DIAGONAL if send_diagonal else ''
        self.Payload = payload
        
    @property
    def is_broadcast(self):
        return self.Dst == self.BROADCAST
        
    @property
    def send_diagonal(self):
        return self.FLAG_DIAGONAL in self.Flags
        
    @property
    def cross_diagonal(self):
        return self.FLAG_CROSS in self.Flags
        
    def to_bytes(self):
        return b"%s:%d:%d:%s:%s" % (to_bytes(self.TID), self.Src, self.Dst, to_bytes(self.Flags),
                to_bytes(self.Payload))

    @staticmethod        
    def from_bytes(buf):
        tid, src, dst, flags, payload = buf.split(":", 4)
        tid = to_str(tid)
        src = int(src)
        dst = int(dst)
        t = Transmission(src, dst, payload)
        t.Flags = to_str(flags)
        t.TID = tid
        return t
