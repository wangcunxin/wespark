# -*- coding: utf-8 -*-
import os
import time
import types

from pyspark import SparkContext, SparkConf

from bblink.website.usercentre.dao import MysqlDao

__author__ = 'kevin'

def my_print(l):
    print l

def my_add(a,b):
    ret=[]
    if type(a)==types.ListType:
        ret = a.append(b)
    else:
        ret = [a,b]

    return ret

def remove_quote(word):
    return word.replace("\"",'')

def normal_mac(mac):
    ar = mac.encode('utf-8')
    rets = None
    if len(ar)==12:
        ret = [ar[0]]
        for i in range(1,len(ar)):
            if i%2==0:

                ret.append(":")
            ret.append(ar[i])
        rets = ''.join(ret)
    else:
        rets = ar
    return rets.upper()

def convert_mac_date(kv):
    k = remove_quote(kv[0])
    v = remove_quote(kv[1])
    return (normal_mac(k),v)

def timestamp(dat):
    '''
    a = dat.split("-")
    dateC=datetime.datetime(int(a[0]),int(a[1]),int(a[2]))
    timestamp= time.mktime(dateC.timetuple())
    '''
    ret = '0'
    try:
        timestamp = time.mktime(time.strptime(dat, '%Y-%m-%d %H:%M:%S'))
        ret = str(long(timestamp))
    except:
        pass
    return ret

def convert_unique_min(kvs):
    k = kvs[0]
    vs = set(kvs[1])
    vs = list(vs)
    list.sort(vs)

    return (k,timestamp(vs[0]))

# combine mysql (mac,firstime) to mysql
if __name__ == '__main__':

    master = "spark://hadoop:7077"
    appName = "spark_combine_mysql"
    '''
    input = '/logs_origin/tmp/user_log.csv'
    input_ = '/logs_origin/tmp/user-0401.csv'

    '''
    input = '/input/combine_mysql/f1'
    input_ = '/input/combine_mysql/f2'

    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home
    conf = (SparkConf()
            .setMaster(master)
            .setAppName(appName)
            )
    sc = SparkContext(conf = conf)

    lines = sc.textFile(input)
    rdd1 = lines.map(lambda line: line.split(","))\
        .filter(lambda arr:len(arr)==18)\
        .map(lambda arr:(arr[8],arr[6]))

    lines = sc.textFile(input_)
    rdd2 = lines.map(lambda line: line.split(","))\
        .filter(lambda arr:len(arr)==33)\
        .map(lambda arr:(arr[24],arr[29]))
    rdd = rdd1.union(rdd2)\
        .map(convert_mac_date)\
        .groupByKey().mapValues(list)\
        .map(convert_unique_min)\
        .filter(lambda (k,v):v!='0')

    print(rdd.count())

    lists = []
    for r in rdd.collect():
        usermac = r[0]
        dat = r[1]
        t = (usermac,dat)
        lists.append(t)

    dao = MysqlDao()
    _sql = "TRUNCATE TABLE tmp_user"
    dao.insert(_sql)

    _sql = "insert into tmp_user(user_mac,create_time) values('%s',%s)"
    #dao.insertMany(_sql, lists)
    for str in lists:
        print(_sql % str)

    sc.stop()
