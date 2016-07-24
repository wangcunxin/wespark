# -*- coding: utf-8 -*-
__author__ = 'kevin'

import time,sched
# only one thread task
if __name__ == '__main__':

    def event_func(msg):
        print "Current Time:",time.time(),'msg:',msg

    #初始化sched模块的scheduler类
    s = sched.scheduler(time.time,time.sleep)
    #设置两个调度
    s.enter(4,2,event_func,("Small event.",))
    s.enter(2,0,event_func,("Big event.",))
    s.run()

    while True:
        time.sleep(2000)
        print "running"
