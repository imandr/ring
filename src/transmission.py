import time, uuid

from .py3 import to_str, to_bytes

#
# Messge types:
#
# Broadcast:
#
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

    def __init__(self, source_id, dest_id, payload, flags=None, encoding="utf-8",
                max_hops = None, max_edge_hops = None, max_diagonal_hops = None, **args):
        t = int(time.time()*1000) % 1000000
        self.TID = "%s.%06d" % (source_id, t)
        self.Src = source_id
        self.Dst = self.BROADCAST if dest_id is None else dest_id 
        if flags is None:
            flags = self.flags(**args)
        self.Flags = flags
        self.Payload = payload.encode(encoding) if isinstance(payload, str) else payload
        self.Encoding = encoding
        self.MaxHops = max_hops
        self.MaxEdgeHops = max_edge_hops
        self.MaxDiagonalHops = max_diagonal_hops
        
    def __str__(self):
        flags = self.Flags
        flags_str = ('s' if flags & self.FLAG_SYSTEM else '-') \
            + ('m' if flags & self.FLAG_MUTABLE else '-') \
            + ('d' if flags & self.FLAG_CROSS_TO_DIAGONAL else '-') \
            + ('e' if flags & self.FLAG_CROSS_TO_EDGE else '-') \
            + ('D' if flags & self.FLAG_SEND_DIAGONAL else '-') \
            + ('E' if flags & self.FLAG_SEND_EDGE else '-')
        hops_limits = "hops:%s/e:%s/d:%s" % (
            str(self.MaxHops) if self.MaxHops is not None else "-",
            str(self.MaxEdgeHops) if self.MaxEdgeHops is not None else "-",
            str(self.MaxDiagonalHops) if self.MaxDiagonalHops is not None else "-"
        )
        return '[Transmission id=%s flags=%s %s->%s %s %s]' % (self.TID, flags_str, self.Src, self.Dst, hops_limits, repr(self.Payload))
        
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
            
    def set_payload(self, payload):
        self.Payload = payload.encode(encoding) if isinstance(payload, str) else payload
            
    def decrement_hops(self):
        if self.MaxHops is not None:
            self.MaxHops -= 1
            return self.MaxHops >= 0
        else:
            return True
        
    def decrement_edge_hops(self):
        if self.MaxEdgeHops is not None:
            self.MaxEdgeHops -= 1
            return self.MaxEdgeHops >= 0
        else:
            return True

    def decrement_diagonal_hops(self):
        if self.MaxDiagonalHops is not None:
            self.MaxDiagonalHops -= 1
            return self.MaxDiagonalHops >= 0
        else:
            return True
        
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
        header = to_bytes(
                "%s:%s:%s:%d:%s:%s:%s:%s|" % (
                        self.TID, self.Src, self.Dst, self.Flags,
                        str(self.MaxHops) if self.MaxHops is not None else "",
                        str(self.MaxEdgeHops) if self.MaxEdgeHops is not None else "",
                        str(self.MaxDiagonalHops) if self.MaxDiagonalHops is not None else "",
                        self.Encoding
                )
        )
        as_bytes = header + self.Payload
        #print("Transmission as bytes:", as_bytes)
        return as_bytes

    @staticmethod        
    def from_bytes(buf):
        # buf is bytes
        assert isinstance(buf, bytes)
        header, payload = buf.split(b'|', 1)
        tid, src, dst, flags, max_hops, max_edge_hops, max_diagonal_hops, encoding = to_str(header).split(":")
        t = Transmission(src, dst, payload,
            max_hops = None if not max_hops else int(max_hops),
            max_edge_hops = None if not max_edge_hops else int(max_edge_hops),
            max_diagonal_hops = None if not max_diagonal_hops else int(max_diagonal_hops),
            encoding = encoding
        )
        t.Flags = int(flags)
        t.TID = tid
        return t

    @property
    def payload_str(self):
        return self.Payload.decode(self.Encoding)
