# -*- coding: utf-8 -*-
from operator import add

import os
import datetime
import random
from pyspark import SparkContext, SparkConf, SQLContext
from arithmetic_week import Arithmetic
#from bblink.jobs.cfilter.base_service import BaseService

__author__ = 'kevin'


SEP=u'-'

def print_str(str):
    print(str)

def convert_week_count(k,vs):
    #(mac,[year-week_ymd,])

    # dic={year-week:ymd,}
    dic = {}
    for v in vs:
        a = v.split("_")
        key = a[0]
        value = a[1]
        if dic.has_key(key):
            dic[key].append(value)
        else:
            dic[key]=[value]
    # [year-week-count,]
    list = []
    for w in dic.keys():
        list.append(w+SEP+str(len(set(dic[w]))))
    # (mac,[week_count,])
    return (k,list)

def mill_date_week(timeStamp):
    mill = long(timeStamp/1000)
    dateArray = datetime.datetime.utcfromtimestamp(mill)
    ret = dateArray.strftime("%Y-%W_%Y%m%d")
    return ret

def convert_week(l):
    year_week = mill_date_week(long(l[2]))
    # (mac,year_week)
    return (l[0],year_week)

def get_user_mark(week_count_list,week_min_max_list,arith):
    ret = True
    mark=u'其他'
    # 0.每周连续:医护
    ret = arith.continue_perweek(week_min_max_list)
    if ret:
        mark = u'医护'
        print mac,ret,mark
    # 1.每周登录大于3:病人
    ret = arith.perweek_morethan_3(week_min_max_list,week_count_list)
    if ret:
        mark = u'病人'
        print mac,ret,mark
    # 2.每2周登录1次:孕8月
    ret = arith.per2weeks_equal_1(week_min_max_list,week_count_list)
    if ret:
        mark = u'孕8月'
        print mac,ret,mark
    # 3.每1周登录1次&周数大于1:孕9月
    ret = arith.perweek_equal_1(week_min_max_list,week_count_list)
    if ret:
        mark = u'孕9月'
        print mac,ret,mark
    # 4.每月(4周)登录1次:孕前期
    ret = arith.per4weeks_equal_1(week_min_max_list,week_count_list)
    if ret:
        mark = u'孕前期'
        print mac,ret,mark

    # 5.首次登录：孕8月 or 孕前期 drop

    return mark


def myrandom(num):
    return int(random.random()*num)

# (mac_hosid,count)
def convert_hosid_count(k,v):
    arr = k.split(SEP)
    mac = arr[0]
    hosid= arr[1]
    # (mac,hosid-count)
    return (mac,hosid+SEP+str(v))

# (mac,[hosid_count,])
def topcount(kvs):
    mac = kvs[0]
    vs = kvs[1]
    dic = {}
    for hosid_count in vs:
        arr = hosid_count.split(SEP)
        hosid = arr[0]
        count = int(arr[1])
        dic[hosid] = count
    max_v=0
    max_k=''
    for (k,v) in dic.items():
       if max_v<v:
            max_v = v
            max_k = k
    best_hosid = max_k

    return (mac,best_hosid)

# analyse (user,area,pregnant_period) base on week
if __name__ == '__main__':
    _sep = "\t"
    master = "local[*]"
    app_name = "user_week_logincount"

    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    input = "/output/back/back-portal-loginlog-parts/20150731/*"
    output = "/output/userareaconceive/01"

    conf = (SparkConf()
            .setMaster(master)
            .setAppName(app_name))
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # mac,user_name,login_time,gwid,hosid
    lines = sc.textFile(input)
    parts = lines.map(lambda l:l.split(_sep))\
        .map(lambda l:(l[0],l[1],l[2],l[3],"hos"+str(myrandom(100))))\
        .filter(lambda l:len(l)==5)\
        .filter(lambda l:len(l[2])==13)

    # user,hosid,count
    hosid_count = parts.map(lambda l:(l[0]+SEP+l[4],1)).reduceByKey(add)\
        .map(lambda (k,v):convert_hosid_count(k,v))
    #hosid_count.foreach(print_str)

    user_top_hosid = hosid_count.groupByKey().mapValues(list).sortByKey()\
        .map(topcount)
    #user_top_hosid.foreach(print_str)


    # user,week,count
    week_count = parts.map(lambda l:convert_week(l))\
        .groupByKey().mapValues(list).sortByKey()\
        .map(lambda (k,vs):convert_week_count(k,vs))

    #week_count.foreach(print_str)

    dic_ret = {}
    arith = Arithmetic()
    for row in week_count.collect():
        mac = row[0]
        week_count_list =row[1]
        # ['2014-48-1','2014-49-1']
        #print mac,week_count_list

        dic = {}
        for i in range(len(week_count_list)):
            dic[i]=int(week_count_list[i].split(SEP)[1])

        week_min_max_list = arith.series_week(dic)
        #['48-49','100-123']
        #print mac,week_min_max_list

        mark = get_user_mark(week_count_list,week_min_max_list,arith)

        dic_ret[mac] = mark

    #print(len(dic_ret))
    for k in dic_ret.keys():
        print k,dic_ret[k]


    # join:hos,age
    ret_lines = []
    for (mac,hosid) in user_top_hosid.collect():
        mark = dic_ret.get(mac,'')
        line = mac+_sep+hosid+_sep+mark
        ret_lines.append(line)

    #BaseService()._write_file(ret_lines,output)


    sc.stop()