import time, uuid
from enum import Enum

from .py3 import to_str, to_bytes

#
# Messge types:
#   
#   guranteed:  no crosing from diag to edge
#               can be waited for (returned to the sender via edge)
#   not guaranteed: allowed to cross from diaginal to edge
#
#   mutable/immutable
#
#   system/user
#

class TransmissionDigest(object):
    
    def __init__(self, transmission):
        self.TID = transmission.TID
        self.Src = transmission.Src
        self.Dst = transmission.Dst
        self.Flags = transmission.Flags
        self.Encoding = transmission.Encoding
        self.Payload = transmission.Payload
        
    @property
    def str(self):
        return self.Payload.decode(self.Encoding)
        
    @property
    def bytes(self):
        return self.Payload
        
    @property
    def is_guaranteed(self):
        return (self.Flags & Transmission.FLAG_GUARANTEED) and True
        
    @property
    def is_mutable(self):
        return (self.Flags & Transmission.FLAG_MUTABLE) and True
        
    @property
    def is_broadcast(self):
        return self.Dst == Transmission.BROADCAST
        
    @property
    def is_direct(self):
        return self.Dst != Transmission.BROADCAST
        
    @property
    def payload_str(self):
        return self.Payload.decode(self.Encoding)

    @property
    def payload(self):
        return self.Payload
        
class Transmission(object):

    BROADCAST = "*"      # dest -> BROADCAST
    
    FLAG_MUTABLE = 1
    FLAG_SYSTEM = 2
    FLAG_GUARANTEED = 4
    
    def __init__(self, source_id, dest_id, payload,
                mutable = False, system = False, guaranteed = False,          # flags
                encoding="utf-8",
                max_hops = None, max_edge_hops = None, max_diagonal_hops = None):
        t = int(time.time()*1000) % 1000000
        self.TID = "%s.%06d" % (source_id, t)
        self.Src = source_id
        self.Dst = self.BROADCAST if dest_id is None else dest_id 
        self.Flags = self.flags(mutable, system, guaranteed)

        if isinstance(payload, str):
            payload = payload.encode(encoding)

        assert isinstance(payload, (bytes, memoryview)), "Unrecognized payload type: %s" % (type(payload),)

        self.Payload = payload
        self.Encoding = encoding
        self.MaxHops = max_hops
        self.MaxEdgeHops = max_edge_hops
        self.MaxDiagonalHops = max_diagonal_hops
        
    @property
    def digest(self):
        return TransmissionDigest(self)
        
    def system(self, value=True):
        if value:
            self.Flags |= self.FLAG_SYSTEM
        else:
            self.Flags &= ~self.FLAG_SYSTEM
        return self
        
    def mutable(self, value=True):
        if value:
            self.Flags |= self.FLAG_MUTABLE
        else:
            self.Flags &= ~self.FLAG_MUTABLE
        return self
        
    def guaranteed(self, value=True):
        if value:
            self.Flags |= self.FLAG_GUARANTEED
        else:
            self.Flags &= ~self.FLAG_GUARANTEED
        return self
        
    @staticmethod
    def broadcast(source_id, payload, **args):
        return Transmission(source_id, Transmission.BROADCAST, payload, **args)
    
    @staticmethod
    def poll(source_id, payload, **args):
        return Transmission.broadcast(source_id, payload, guaranteed=True, **args)
        
    @staticmethod
    def direct(source_id, dest_id, payload, **args):
        return Transmission(source_id, dest_id, payload, **args)
        
    def __str__(self):
        flags = self.Flags
        flags_str = ('S' if flags & self.FLAG_SYSTEM else 's') \
            + ('M' if flags & self.FLAG_MUTABLE else 'm') \
            + ('G' if flags & self.FLAG_GUARANTEED else 'g')              
        hops_limits = "hops:%s/e:%s/d:%s" % (
            str(self.MaxHops) if self.MaxHops is not None else "-",
            str(self.MaxEdgeHops) if self.MaxEdgeHops is not None else "-",
            str(self.MaxDiagonalHops) if self.MaxDiagonalHops is not None else "-"
        )
        return '[Transmission id=%s f=%s %s->%s %s %s]' % (self.TID, flags_str, self.Src, self.Dst, hops_limits, repr(self.Payload))
        
    @staticmethod
    def flags(mutable = False, system = False, guaranteed = False):
        return 0 \
            | (guaranteed and Transmission.FLAG_GUARANTEED) \
            | (mutable and Transmission.FLAG_MUTABLE) \
            | (system and Transmission.FLAG_SYSTEM)
            
    def decrement_hops(self):
        if self.MaxHops is not None:
            self.MaxHops -= 1
            return self.MaxHops >= 0
        else:
            return True
        
    def decrement_edge_hops(self):
        if self.MaxHops is None or self.MaxHops > 0:
            if self.MaxEdgeHops is None or self.MaxEdgeHops > 0:
                if self.MaxHops is not None:        self.MaxHops -= 1
                if self.MaxEdgeHops is not None:    self.MaxEdgeHops -= 1
                return True
        else:
            return False

    def decrement_diagonal_hops(self):
        if self.MaxHops is None or self.MaxHops > 0:
            if self.MaxDiagonalHops is None or self.MaxDiagonalHops > 0:
                if self.MaxHops is not None:        self.MaxHops -= 1
                if self.MaxDiagonalHops is not None:    self.MaxDiagonalHops -= 1
                return True
        else:
            return False

    @property
    def is_broadcast(self):
        return self.Dst == self.BROADCAST
        
    @property
    def is_direct(self):
        return self.Dst != self.BROADCAST
        
    @property
    def is_mutable(self):
        return (self.FLAG_MUTABLE & self.Flags) and True
        
    @property
    def is_system(self):
        return (self.FLAG_SYSTEM & self.Flags) and True

    @property
    def is_guaranteed(self):
        return (self.FLAG_GUARANTEED & self.Flags) and True

    @property
    def is_guaranteed(self):
        return not (self.FLAG_GUARANTEED & self.Flags)

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

    @property
    def payload(self):
        return self.Payload
        
    @payload.setter
    def payload(self, payload):
        if isinstance(payload, str):
            payload = payload.encode(self.Encoding)
        self.Payload = payload