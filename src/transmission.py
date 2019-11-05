import time, uuid

from py3 import to_str, to_bytes

#
# Messge types:
#
# Runner: broadcast, send_diagonal = False
#    mutable
#    immutable
#.   can be confirmed
#
# Shout: broadcast, send_diagonal = True, cross_to_diagonal = True, cross_to_edge = True
#.   can not be confirmed
#
# Point-to-point: send_diagonal = True, cross_to_diagonal = True, cross_to_edge = True
#

class Transmission(object):

    BROADCAST = "*"      # dest -> BROADCAST
    
    FLAG_SEND_EDGE = 1
    FLAG_SEND_DIAGONAL = 2
    FLAG_CROSS_TO_EDGE = 4
    FLAG_CROSS_TO_DIAGONAL = 8
    FLAG_MUTABLE = 16
    FLAG_SYSTEM = 32
    
    FLAGS_DEFAULT = FLAG_SEND_EDGE | FLAG_SEND_DIAGONAL | FLAG_CROSS_TO_DIAGONAL | FLAG_CROSS_TO_EDGE
    FLAGS_RUNNER = FLAG_SEND_EDGE
    FLAGS_SHOUT = FLAG_SEND_EDGE | FLAG_SEND_DIAGONAL | FLAG_CROSS_TO_DIAGONAL | FLAG_CROSS_TO_EDGE
    FLAGS_POINT_TO_POINT = FLAG_SEND_EDGE | FLAG_SEND_DIAGONAL | FLAG_CROSS_TO_DIAGONAL | FLAG_CROSS_TO_EDGE

    def __init__(self, source_id, dest_id, payload, flags=None, **args):
        t = time.time()
        self.TID = "%s.%d.%d" % (source_id, int(t), int((t-int(t))*1000000))
        self.Src = source_id
        self.Dst = self.BROADCAST if dest_id is None else dest_id 
        if flags is None:
            flags = self.flags(**args)
        self.Flags = flags
        self.Payload = payload
        
    def __str__(self):
        flags = self.Flags
        flags_str = ('s' if flags & self.FLAG_SYSTEM else '-') \
            + ('m' if flags & self.FLAG_MUTABLE else '-') \
            + ('d' if flags & self.FLAG_CROSS_TO_DIAGONAL else '-') \
            + ('e' if flags & self.FLAG_CROSS_TO_EDGE else '-') \
            + ('D' if flags & self.FLAG_SEND_DIAGONAL else '-') \
            + ('E' if flags & self.FLAG_SEND_EDGE else '-')
        return '[Transmission id=%s flags=%s %s->%s %s]' % (self.TID, flags_str, self.Src, self.Dst, repr(self.Payload))
        
    @staticmethod
    def flags(send_edge = True, send_diagonal = True, cross_to_edge = True, cross_to_diagonal = True,
                mutable = False, system = False):
        return \
            (Transmission.FLAG_SEND_EDGE if send_edge else 0) \
            | (Transmission.FLAG_SEND_DIAGONAL if send_diagonal else 0) \
            | (Transmission.FLAG_CROSS_TO_EDGE if cross_to_edge else 0) \
            | (Transmission.FLAG_CROSS_TO_DIAGONAL if cross_to_diagonal else 0) \
            | (Transmission.FLAG_MUTABLE if mutable else 0) \
            | (Transmission.FLAG_SYSTEM if system else 0)
        
    @property
    def broadcast(self):
        return self.Dst == self.BROADCAST
        
    @property
    def send_diagonal(self):
        return (self.FLAG_SEND_DIAGONAL & self.Flags) != 0
        
    @property
    def cross_to_diagonal(self):
        return (self.FLAG_CROSS_TO_DIAGONAL & self.Flags) != 0
        
    @property
    def send_edge(self):
        return (self.FLAG_SEND_EDGE & self.Flags) != 0
        
    @property
    def cross_to_edge(self):
        return (self.FLAG_CROSS_TO_EDGE & self.Flags) != 0
        
    @property
    def mutable(self):
        return (self.FLAG_MUTABLE & self.Flags) != 0
        
    @property
    def system(self):
        return (self.FLAG_SYSTEM & self.Flags) != 0

    @property
    def runner(self):
        return self.broadcast and not self.send_diagonal
        
    def to_bytes(self):
        #print("flags:", self.Flags, type(self.Flags))
        #print("args:",to_bytes(self.TID), self.Src, self.Dst, self.Flags,
        #        to_bytes(self.Payload))
        return b"%s:%s:%s:%d:%s" % (to_bytes(self.TID), to_bytes(self.Src), to_bytes(self.Dst), self.Flags,
                to_bytes(self.Payload))

    @staticmethod        
    def from_bytes(buf):
        buf = to_str(buf)
        #print("from_bytes: buf:[%s]" % (buf,))
        tid, src, dst, flags, payload = buf.split(":", 4)
        tid = tid
        src = src
        dst = dst
        t = Transmission(src, dst, payload)
        t.Flags = int(flags)
        t.TID = tid
        return t
