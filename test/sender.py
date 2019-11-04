from socket import *
import sys, time

s = socket(AF_INET, SOCK_STREAM)
s.connect((sys.argv[1], int(sys.argv[2])))

nsent = 0

while True:
    n = s.send(b'.')
    nsent += n
    print ("sent:", nsent)    
    n = s.send(b"x")
    nsent += n
    print ("sent:", nsent)
    time.sleep(3)