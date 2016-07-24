#encoding: utf-8
import threading,time

class Timer(threading.Thread):
    def __init__(self,fn,args=(),sleep=1,lastDo=True):
        threading.Thread.__init__(self)
        self.fn = fn
        self.args = args
        self.sleep = sleep
        self.lastDo = lastDo
        self.setDaemon(True)

        self.isPlay = True
        self.fnPlay = False

    def __do(self):
        self.fnPlay = True
        apply(self.fn,self.args)
        self.fnPlay = False

    def run(self):
        while self.isPlay :
            self.__do()
            time.sleep(self.sleep)

    def stop(self):
        #stop the loop
        self.isPlay = False
        while True:
            if not self.fnPlay : break
            time.sleep(0.01)
        #if lastDo,do it again
        if self.lastDo : self.__do()

if __name__ == '__main__':

    i = 0
    cond = threading.Event()
    def update_self():
        global i
        global cond
        try:
            print i
            i = i + 1
            if i>3:
                #self.stop() #global name self is not defined
                cond.set()
                print 'event set\n'
        except Exception,e:
            print Exception, e


    update_self_handle = Timer(fn=update_self, sleep=1)
    update_self_handle.daemon = True
    update_self_handle.start()
    print 'main start waiting\n'
    cond.wait()
    print 'main come here, exiting...\n'