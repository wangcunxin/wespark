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
    # k = phone-mac
    k = remove_quote(kv[0])+'-'+normal_mac(remove_quote(kv[2]))
    v = remove_quote(kv[1])
    return (k,v)

def timestamp(dat):
    '''
    a = dat.split("-")
    dateC=datetime.datetime(int(a[0]),int(a[1]),int(a[2]))
    timestamp= time.mktime(dateC.timetuple())
    '''
    ret = '0'
    _dat=dat
    t = type(dat)
    if t==types.UnicodeType:
        _dat= dat.encode('utf-8')
    else:
        _dat=str(dat)
    try:
        timestamp = time.mktime(time.strptime(_dat, '%Y-%m-%d %H:%M:%S'))
        ret = str(long(timestamp))
    except:
        ret = '0'
        pass
    return ret

def convert_unique_min(kvs):
    k = kvs[0]
    vs = set(kvs[1])
    vs = list(vs)
    list.sort(vs)

    return (k,timestamp(vs[0]))

def filter_type(v):
    ret = False
    str = v.encode('utf-8')
    if (str=='"MAC"' or str=='"MOBILE"'):
        ret = True
    return ret

def filter_len(v):
    ret = False
    #phone,time,mac
    if ((len(v[0])==13) and (len(v[1])==21)):
        ret = True
    return ret

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
        .filter(lambda arr:filter_type(arr[4]))\
        .map(lambda arr:(arr[5],arr[6],arr[8]))
    #phone,time,mac
    lines = sc.textFile(input_)
    rdd2 = lines.map(lambda line: line.split(","))\
        .filter(lambda arr:len(arr)==33)\
        .filter(lambda arr:filter_type(arr[1]))\
        .map(lambda arr:(arr[3],arr[29],arr[24]))
    '''
    rdd = rdd1.union(rdd2).collect()
    for s in rdd:
        print(s)
    '''
    rdd = rdd1.union(rdd2)\
        .filter(filter_len)\
        .map(convert_mac_date)\
        .groupByKey().mapValues(list)\
        .map(convert_unique_min)\
        .filter(lambda (k,v):v!='0')

    #print(rdd.count())

    lists = []
    for r in rdd.collect():
        kv = r[0].split('-')
        phone = kv[0]
        usermac = kv[1]
        dat = r[1]
        t = (usermac,dat,phone)
        lists.append(t)
    print lists
    dao = MysqlDao()
    _sql = "TRUNCATE TABLE tmp_user_mobile"
    dao.insert(_sql)

    _sql = "insert into tmp_user_mobile(user_mac,create_time,user_mobile) values('%s',%s,'%s')"
    #dao.insertMany(_sql, lists)
    for str in lists:
        print(_sql % str)

    #print("---->finished")


    sc.stop()
