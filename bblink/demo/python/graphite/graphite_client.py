# -*- coding: utf-8 -*-
import socket
import time
import random


CARBON_SERVER = '112.65.205.79'
CARBON_PORT = 2003

def sendmsg(msg=''):
    sock = socket.socket()
    sock.connect((CARBON_SERVER, CARBON_PORT))
    sock.sendall(message)
    sock.close()

for i in range(1,10000,1):
    inttime=int(time.time())
    #message = 'testdataoffset.data %d %d\n' % (random.randint(1,50),int(time.time())-172800)
    message = 'withcnname.henanfuyou.lenglu.data %d %d\n' % (random.randint(1,50),inttime)
    sendmsg(message)
    message = 'withcnname.henanfuyou.netflow.data %d %d\n' % (random.randint(1,50),inttime)
    sendmsg(message)
    message = 'withcnname.henanfuyou.pv.data %d %d\n' % (random.randint(1,50),inttime)
    sendmsg(message)
    message = 'withcnname.henanfuyou.uv.data %d %d\n' % (random.randint(1,50),inttime)
    sendmsg(message)
    message = 'withcnname.henanfuyou.loginuv.data %d %d\n' % (random.randint(1,50),inttime)
    sendmsg(message)
    message = 'withcnname.henanfuyou.loginpv.data %d %d\n' % (random.randint(1,50),inttime)
    sendmsg(message)
    message = 'withcnname.henanfuyou.authuv.data %d %d\n' % (random.randint(1,50),inttime)
    sendmsg(message)
    message = 'withcnname.henanfuyou.authpv.data %d %d\n' % (random.randint(1,50),inttime)
    sendmsg(message)

    print 'sending message:\n%s' % message

    time.sleep(1)
