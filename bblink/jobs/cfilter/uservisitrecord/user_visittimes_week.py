# encoding: utf-8

import os, sys, datetime, time
import random

from pyspark.sql.types import StructField, IntegerType, StringType, StructType, LongType
from pyspark import SparkContext, SparkConf, SQLContext
from bblink.back.logger import *

SEP='_'
SEP_TAB='\t'
SIGN_FIRST=u'first'
SIGN_LAST=u'last'

def my_print(l):
    print l

def date_week(day):
    date = datetime.datetime.strptime(day, "%Y%m%d")
    ret = date.strftime("%Y-%W")
    return ret

def subStr(s):

    return s[8:10]

def convert_kv_first(r):
    #Row(day=u'20151201', mac=u'7C7D3D0FEF6B', hosid=u'112',
    #  firstTime=u'2015120118', lastTime=u'2015120118', dat=20160101)
    year_week = date_week(r[0])

    return (year_week+SEP+r[1]+SEP+r[2],subStr(r[3]))

def convert_kv_last(r):
    year_week = date_week(r[0])

    return (year_week+SEP+r[1]+SEP+r[2],subStr(r[4]))

def times_count_first(kvs):
    #(u'2015-50_7014A62FA5B0_0', [u'22',u'23'])
    dic = {}
    for k in kvs[1]:
        if dic.has_key(k):
            dic[k]+=1
        else:
            dic[k]=1
    ret = []
    for k in dic.keys():
        ret.append(SIGN_FIRST+SEP+k+SEP+str(dic[k]))
    list.sort(ret)
    return (kvs[0],ret)

def times_count_last(kvs):
    #(u'2015-50_7014A62FA5B0_0', [u'22',u'23'])
    dic = {}
    for k in kvs[1]:
        if dic.has_key(k):
            dic[k]+=1
        else:
            dic[k]=1
    ret = []
    for k in dic.keys():
        ret.append(SIGN_LAST+SEP+k+SEP+str(dic[k]))
    list.sort(ret)
    return (kvs[0],ret)

def getRandom(i):
    return int(random.random()*10000)% i

def convert_rets(kvs):
    rets =[]
    #(u'2015-48_A43D788903B8_142', ([u'first_16_1'], [u'last_20_1']))
    k = kvs[0].split(SEP)
    year_week = k[0].split('-')
    ret_keys = [year_week[0],year_week[1],k[1],k[2]]
    tmp_list =[]
    firsts = kvs[1][0]
    lasts = kvs[1][1]
    size = len(firsts) if len(firsts)<=len(lasts) else len(lasts)
    for i in range(0,size):
        #u'first_16_1'
        first = firsts[i]
        last = [i]
        a = first.split(SEP)
        b = last.split(SEP)
        tmp_list = ret_keys+[a[1],int(a[2]),b[1],int(b[2])]
        rets.append(tuple(tmp_list))

    return (getRandom(3),rets)

__author__ = 'kevin'

'''
analyse:
mid_uservisitpage_day:<user,1,0,1>
mid_uservisittime_day:<user,'',''>
'''
if __name__ == '__main__':
    '''
    if len(sys.argv) != 3:
        print("Usage: user_*.py <input>")
        sys.exit(-1)

    date = sys.argv[1]
    master = sys.argv[2]
    '''
    day = "20160101"
    master = "local[*]"

    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    input = "/output/mid_uservisittime_day/dat=%s" % day
    output = "/output/mid_uservisittime_week/dat=%s" % day
    conf = (SparkConf()
            .setMaster(master)
            .setAppName("user_visit_week")
            #.set("spark.kryoserializer.buffer.mb", "256")
            .set("spark.sql.parquet.binaryAsString", "true"))
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    df = sqlContext.read.parquet(input)
    #df.registerTempTable('mid_uservisittime_day')
    fields = [
        StructField('day', StringType(), True),
        StructField('mac', StringType(), True),
        StructField('hosid', StringType(), True),
        StructField('firstTime', StringType(), True),
        StructField('lastTime', StringType(), True)
    ]
    schema = StructType(fields)

    rdd1_1 = df.rdd.map(convert_kv_first)
    #(u'2015-48_6C25B958F2CC_175', u'2015120120')
    #rdd1.foreach(my_print)
    #(u'2015-50_7014A62FA5B0_0', [u'22',u'23'])
    rdd1_2 = rdd1_1.groupByKey().mapValues(list).sortByKey().map(times_count_first)
    #(u'2015-48_903C920CAE97_655', [u'15_1',u'16_1'])
    #rdd1_2.foreach(my_print)

    rdd2_1 = df.rdd.map(convert_kv_last)
    rdd2_2 = rdd2_1.groupByKey().mapValues(list).sortByKey().map(times_count_last)

    rdd3 = rdd1_2.join(rdd2_2).map(convert_rets).values().flatMap(list)
    #(u'2015', u'48', u'A09347EC9FBB', u'189', u'13', u'1', u'14', u'1')
    #rdd3.foreach(my_print)

    fields = [
        StructField('year', StringType(), True),
        StructField('week', StringType(), True),
        StructField('mac', StringType(), True),
        StructField('hosid', StringType(), True),
        StructField('firstTime', StringType(), True),
        StructField('firstCount', LongType(), True),
        StructField('lastTime', StringType(), True),
        StructField('lastCount', LongType(), True)
    ]
    schema = StructType(fields)

    df1 =  sqlContext.createDataFrame(rdd3,schema)
    df1.coalesce(2).write.parquet(output,'overwrite')


    sc.stop()