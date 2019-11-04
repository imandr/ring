from socket import *
import sys, time

s = socket(AF_INET, SOCK_STREAM)
s.bind(('', int(sys.argv[1])))
s.listen()

while True:
    s1, addr = s.accept()
    print("accepted")
    eof = False
    n = 10
    while n > 0 and not eof:
        msg = s1.recv(10000)
        if len(msg):
            print(msg)
        else:
            eof = True
        n -= len(msg)
    print('closed')
    s1.close()