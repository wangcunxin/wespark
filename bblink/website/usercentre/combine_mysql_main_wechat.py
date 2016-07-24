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
    # k = mac-open_id-app_id
    k = normal_mac(remove_quote(kv[0]))+'-'+remove_quote(kv[1])+'-'+remove_quote(kv[2])
    v = remove_quote(kv[3])
    return (k,v)

def convert_unique_min(kvs):
    k = kvs[0]
    vs = set(kvs[1])
    vs = list(vs)
    list.sort(vs)

    return (k,vs[0])


# combine mysql (mac,firstime) to mysql
if __name__ == '__main__':

    master = "spark://hadoop:7077"
    appName = "spark_combine_mysql"
    '''
    input = '/logs_origin/tmp/log_wechat_attention_20160411.csv'

    '''
    input = '/input/combine_mysql/f3'

    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home
    conf = (SparkConf()
            .setMaster(master)
            .setAppName(appName)
            )
    sc = SparkContext(conf = conf)

    lines = sc.textFile(input)
    rdd = lines.map(lambda line: line.split(","))\
        .filter(lambda arr:len(arr)==4)\
        .map(convert_mac_date)\
        .groupByKey().mapValues(list)\
        .map(convert_unique_min)

    #print(rdd.count())

    lists = []
    for r in rdd.collect():
        # k = mac-open_id-app_id
        kv = r[0].split('-')
        usermac = kv[0]
        open_id = kv[1]
        app_id = kv[2]
        dat = r[1]
        t = (usermac,open_id,app_id,dat)
        lists.append(t)
    #print lists
    dao = MysqlDao()
    _sql = "TRUNCATE TABLE tmp_user_wechat"
    dao.insert(_sql)

    _sql = "insert into tmp_user_wechat(user_mac,user_src,open_id,app_id,create_time) values('%s','wifi_wechat','%s','%s',%s);"
    #dao.insertMany(_sql, lists)
    for str in lists:
        print(_sql % str)

    #print("---->finished")

    sc.stop()
