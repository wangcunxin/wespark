import threading
import time

__author__ = 'kevin'

def timer_start():
    t = threading.Timer(5, event_func, ("msg1", "msg2"))
    t.start()


def event_func(msg1, msg2):
    print "I execute:", msg1, msg2
    timer_start()


# threading:many threads
if __name__ == '__main__':

    timer_start()

    while True:
        time.sleep(20)
        print 'main running'
