import time
from socket import timeout as socket_timeout
from socket import socket, SOCK_STREAM, AF_INET, MSG_WAITALL, MSG_PEEK, MSG_DONTWAIT

class StreamTimeout(Exception):
    def __init__(self, message):
        self.Message = message
        
    def __str__(self):
        return self.Message
    
class ProtocolError(Exception):
    def __init__(self, header):
        self.Header = header
        
    def __str__(self):
        return "ProtocolError: header: %s" % (self.Header,)

def to_bytes(s):
    if isinstance(s, str):
        s = bytes(s, "utf-8")
    return s
    
def to_str(b):
    if isinstance(b, bytes):
        b = b.decode("utf-8")
    return b
    
class SocketTimeout(object):
    
    def __init__(self, sock, timeout="just_save"):
        self.Sock = sock
        self.SavedTimeout = None
        self.Timeout = timeout
        
    def __enter__(self):
        self.SavedTimeout = self.Sock.gettimeout()
        if self.Timeout != "just_save":
            self.Sock.settimeout(self.Timeout)
        return self
        
    def __exit__(self, *args):
        try:
            self.Sock.settimeout(self.SavedTimeout)
        except:
            #print(*args)
            pass    # socker may be closed

class MessageStream(object):
    
    VERSION = "1.0"
    
    def __init__(self, sock=None, name=None):
        self.Name = name
        self.Sock = sock
        self.Closed = False
    
    def __str__(self):
        name = f'"{self.Name}"' or ("%x" % (id(self),))
        if self.Sock is not None:
            try:    my_addr = self.Sock.getsockname()
            except: my_addr = "?"
            try:    peer_addr = self.Sock.getpeername()
            except: peer_addr = "?"
            name += f" {peer_addr}->{my_addr}"
        else:
            name += " (no socket)"
        return f"MessageStream ({name})"
        
    def connect(self, addr, tmo=None):
        sock = socket(AF_INET, SOCK_STREAM)
        with SocketTimeout(sock, tmo):
            #print("connecting to:", sock_or_addr)
            try:    sock.connect(addr)
            except socket_timeout:
                raise StreamTimeout()
            self.Sock = sock

    def eof(self):      # compatibility
        return self.Closed
        
    @property
    def EOF(self):
        return self.Closed
        
    def fileno(self):
        return None if (self.Closed or self.Sock is None) else self.Sock.fileno()

    def send(self, msg, tmo=None):
        if self.Closed: return False
        header = "M:%s:%d|" % (self.VERSION, len(msg))
        with SocketTimeout(self.Sock, tmo):
            try:        
                self.Sock.sendall(to_bytes(header)+to_bytes(msg))
            except socket_timeout:
                raise StreamTimeout(f"{self}: timeout sending message")
            except Exception as e:
                #print(f"{self} send(): closing due to exception:", e)
                self.close()
                return False
        return True
        
    def zing(self):
        self.Sock.sendall(b"Z:")
        
    def peek(self, tmo=None):
        with SocketTimeout(self.Sock, tmo):
            try:
                b = self.Sock.recv(1, MSG_PEEK)
            except socket_timeout:
                return False
            except:
                pass
            return True             # True if there is something to read or EOF
        
    def _read_n(self, n, t1=None):
        #print(f"MessageStream._read_n({n})")
        if n <= 0 or self.Closed:  return b''
        t0 = time.time()
        nread = 0
        n_to_read = n
        data_read = b''
        with SocketTimeout(self.Sock):
            try:
                while not self.Closed and (t1 is None or time.time() < t1) and n_to_read > 0:
                    d = None if t1 is None else max(0.0, t1-time.time())
                    self.Sock.settimeout(d)
                    data = self.Sock.recv(n_to_read, MSG_WAITALL)
                    if not len(data):
                        self.close()
                        return data_read
                    data_read += data
                    n_to_read -= len(data)
                #print("MessageStream._read_n: end of loop. received:", data_read)
            except socket_timeout:
                raise StreamTimeout(f"{self}: timeout in _read_n")
            except Exception as e:
                #print(f"{self} _read_n(): closing due to exception", e)
                self.close()
        return data_read
        
    def _read_until(self, end, t1=None):
        data_read = b''
        c = b''
        with SocketTimeout(self.Sock):
            try:
                while not self.Closed and c != end:
                    if t1 is not None and time.time() >= t1:
                        print(f"{self} timed out")
                        break
                    d = None if t1 is None else max(0.0, t1-time.time())
                    self.Sock.settimeout(d)
                    retry = 1
                    while retry > 0:
                        try:    c = self.Sock.recv(1)
                        except: c = b''
                        if len(c):
                            break
                        else:
                            retry -= 1
                    if not len(c):
                        #print(f"{self}: _read_until: EOF")
                        self.close()
                        return data_read
                    data_read += c
                #print("MessageStream._read_until: end of loop. received:", data_read)
            except socket_timeout:
                raise StreamTimeout(f"{self}: timeout in _read_until")
            except Exception as e:
                #print(f"{self} _read_until(): closing due to exception", e)
                self.close()
                raise
        #print("MessageStream._read_until: returning:", data_read)
        return data_read
        
    def _recv_msg(self, t1=None):
        h = self._read_until(b'|', t1)
        if len(h) == 0 or h[-1:] != b'|':
            #print(f"{self}: incomplete or empty header: [{h}]")
            return None, None     # EOF
        message_type, version, size = h[:-1].split(b':')
        version = to_str(version)
        size = int(to_str(size))
        #print("_recv_msg: type, version, size=", message_type, version, size)
        if message_type == b'Z':
            return 'Z', None
        if message_type == b'z':
            return 'z', None
        elif message_type == b'M':
            #header = "M:%10s:%020d:" % (self.VERSION, len(msg))
            data = self._read_n(size) if size > 0 else b''
            #print("_recv_msg: received data:", data)
            return 'M', data
        else:
            raise ProtocolError(h)
            
    def recv(self, tmo=None):
        
        if not self.peek(tmo):
            raise StreamTimeout(f"{self}: timeout in initial peek() in recv()")

        t, data = self._recv_msg()
        if t == 'Z':
            self.Sock.sendall(b'z:')            # send zong
        elif t == 'z':
            pass                                # ignore zongs
        elif t == 'M':
            #print("MessageStream.recv: receited:", data)
            return data
        elif t is None:
            return None
        else:
            raise ProtocolError(t)
        
    def sendAndRecv(self, msg, tmo=None):
        #print("sendAndRecv: msg:", msg)
        self.send(msg, tmo)
        reply = self.recv(tmo)
        #print("sendAndRecv: reply:", reply)
        return reply
        
    def close(self):
        self.Closed = True
        if self.Sock is not None:
            #print(f"{self}: close()")
            self.Sock.close()
        self.Sock = None

    def __del__(self):
        self.close()
        
        
if __name__ == '__main__':
    import sys
    from socket import *
    
    if sys.argv[1] == 'server':
        sock = socket(AF_INET, SOCK_STREAM)
        sock.bind(('', 3456))
        sock.listen()
        s, addr = sock.accept()
        stream = MessageStream(s)
        while True:
            stream.zing()
            msg = stream.recv()
            print ("server: received:", msg)
            if msg is None:
                print("EOF")
                break
            stream.send(b"echo: "+msg)
    
    else:
        stream = MessageStream.connect(('127.0.0.1', 3456))
        while not stream.Closed:
            reply = stream.sendAndRecv("hello %f" % (time.time(),))
            if reply is None:
                print("EOF")
            else:
                print("client: reply:", reply)
                time.sleep(3)
        
            
            