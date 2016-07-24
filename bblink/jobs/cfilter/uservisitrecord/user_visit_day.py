# encoding: utf-8

import os, sys, datetime, time

from pyspark.sql.types import StructField, IntegerType, StringType, StructType, LongType
from pyspark import SparkContext, SparkConf, SQLContext
#from bblink.jobs.cfilter.logger import *

SEP='_'
SEP_TAB='\t'

def timestamp_ymd(longtime=0):

    return datetime.datetime.fromtimestamp(long(longtime / 1000)).strftime('%Y%m%d')

def timestamp_ymdh(longtime=0):

    return datetime.datetime.fromtimestamp(long(longtime / 1000)).strftime('%Y%m%d%H')

def convert_logtime(l):
    ymd = timestamp_ymd(l[4])
    return (l[0],l[1],l[2],l[3],ymd,l[5].upper().replace(':',''))

def convert_kv(l):
    #'logintype', 'logtype', 'hosid', 'suppid', 'logtime', 'usermac

    return (l[4]+SEP+l[5]+SEP+l[2],l[1])

def convert_set(kvs):
    vs = set(kvs[1])
    return (kvs[0],vs)

def convert_visitpage(kvs):
    #(u'20151201_74:AD:B7:78:03:86_119', set([u'1-prelogin', u'2-mobile-login']))
    keys=kvs[0].split(SEP)
    logtype_set=kvs[1]
    rets = [keys[0],keys[1],keys[2]]

    r0 = 0
    r1 = 1
    if logtype_set.__contains__("2-wechat-login") | logtype_set.__contains__("2-mobile-login"):
        rets.append(r1)
    else:
        rets.append(r0)

    if logtype_set.__contains__("3-wehcat-forward") | logtype_set.__contains__("3-mobile-forward"):
        rets.append(r1)
    else:
        rets.append(r0)

    if logtype_set.__contains__("5-wechat-arrive") | logtype_set.__contains__("5-mobile-arrive"):
        rets.append(r1)
    else:
        rets.append(r0)

    return tuple(rets)

def my_print(l):
    print l

def convert_kv2(l):
    #'logintype', 'logtype', 'hosid', 'suppid', 'logtime', 'usermac
    ymd = timestamp_ymd(l[4])

    return (ymd+SEP+l[5].upper().replace(':','')+SEP+l[2],timestamp_ymdh(l[4]))

def convert_sort(kvs):
    vs = set(kvs[1])
    vs = list(vs)
    list.sort(vs)
    return (kvs[0],vs)

def convert_days(kvs):

    keys=kvs[0].split(SEP)
    day_list=kvs[1]
    rets = [keys[0],keys[1],keys[2]]
    size = len(day_list)

    begin=u''
    end=u''
    if size==1:
        begin=day_list[0]
        end=day_list[0]
    elif size==2:
        begin=day_list[0]
        end=day_list[1]
    elif size>=3:
        begin=day_list[0]
        end=day_list[size-1]

    #rets.append(begin).append(end)
    rets+=[begin,end]
    return tuple(rets)

__author__ = 'kevin'
'''
analyse:
mid_uservisitpage_day:<user,1,0,1>
mid_uservisittime_day:<user,'',''>
'''
if __name__ == '__main__':
    '''
    dfrom,logtype,hosid,suppid,gwid,
    usermac,loginversion,forwardversion,arriveversion,logintype,
    isnewuser,logtime
    '''

    '''
    if len(sys.argv) != 3:
        print("Usage: user_*.py <input>")
        sys.exit(-1)

    day = sys.argv[1]
    master = sys.argv[2]
    '''
    day = "20160102"
    master = "local[*]"

    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    #input = "/impala/parquet/back/back-portal-loginflowlog/dat=%s" % day
    input = "/input/loginfowlog/02*"
    #output = "/impala/parquet/back"
    output = "/output"
    conf = (SparkConf()
            .setMaster(master)
            .setAppName("user_visit_day")
            #.set("spark.kryoserializer.buffer.mb", "256")
            .set("spark.sql.parquet.binaryAsString", "true"))
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    df = sqlContext.read.parquet(input)
    rdd = df.select('logintype', 'logtype', 'hosid', 'suppid', 'logtime', 'usermac')

    fields = [
        StructField('day', StringType(), True),
        StructField('mac', StringType(), True),
        StructField('hosid', StringType(), True),
        StructField('loginPage', IntegerType(), False),
        StructField('forwardPage', IntegerType(), False),
        StructField('arrivePage', IntegerType(), False)
    ]
    schema = StructType(fields)

    # compute pages
    rdd1 = rdd.map(convert_logtime)\
        .map(convert_kv)\
        .groupByKey().mapValues(list).map(convert_set)
    #(u'20151201_74:AD:B7:78:03:86_119', set([u'1-prelogin', u'2-mobile-login']))

    rdd1_2 = rdd1.map(convert_visitpage)
    df1 =  sqlContext.createDataFrame(rdd1_2,schema)
    #.registerTempTable("mid_uservisitpage_day")
    _output = output+"/mid_uservisitpage_day/dat=%s" % day
    df1.coalesce(2).write.parquet(_output,'overwrite')


    # compute times
    rdd2 = rdd.map(convert_kv2).groupByKey().mapValues(list).map(convert_sort)
    # (u'20151201_38AA3C3DBC12_127', ['2015120119', '2015120121'])
    rdd2_2 = rdd2.map(convert_days)
    #rdd2_2.foreach(my_print)
    fields = [
        StructField('day', StringType(), True),
        StructField('mac', StringType(), True),
        StructField('hosid', StringType(), True),
        StructField('firstTime', StringType(), True),
        StructField('lastTime', StringType(), True)
    ]
    schema = StructType(fields)
    df2 =  sqlContext.createDataFrame(rdd2_2,schema)
    _output = output+"/mid_uservisittime_day/dat=%s" % day
    df2.coalesce(2).write.parquet(_output,'overwrite')

    sc.stop()