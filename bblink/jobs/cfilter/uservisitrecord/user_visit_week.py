# encoding: utf-8

import os, sys, datetime, time

from pyspark.sql.types import StructField, IntegerType, StringType, StructType, LongType
from pyspark import SparkContext, SparkConf, SQLContext
from bblink.jobs.cfilter.logger import *

SEP='_'
SEP_TAB='\t'

def my_print(l):
    print l

def mill_date_week(timeStamp):
    mill = long(timeStamp/1000)
    dateArray = datetime.datetime.utcfromtimestamp(mill)
    ret = dateArray.strftime("%Y-%W_%Y%m%d")
    return ret

def convert_week(l):
    #'logintype', 'logtype', 'hosid', 'suppid', 'logtime', 'usermac
    year_week_day = mill_date_week(long(l[4])).split(SEP)
    src_logtype=l[1]
    ret_logtype=src_logtype
    # combine
    if src_logtype=="2-wechat-login" or src_logtype=="2-mobile-login":
        ret_logtype = "2-*-login"
    elif src_logtype=="3-wehcat-forward" or src_logtype=="3-mobile-forward":
        ret_logtype = "3-*-forward"
    elif src_logtype=="5-wechat-arrive" or src_logtype=="5-mobile-arrive":
        ret_logtype = "5-*-arrive"

    # (usermac_hosid_year-week,logtype_day)
    return (l[5].upper().replace(':','')+SEP+l[3]+SEP+year_week_day[0],ret_logtype+SEP+year_week_day[1])


def convert_set(kvs):
    vs = set(kvs[1])
    weeks = list(vs)
    list.sort(weeks)
    return (kvs[0],weeks)

def convert_visit_days(kvs):
    #(u'6C72E76890FC_1_2015-48', [u'1-prelogin_20151201', u'2-mobile-login_20151201'])
    keys=kvs[0].split(SEP)
    arr=kvs[1]
    rets = [keys[0],keys[1],keys[2]]
    s1 = "2-*-login"
    s2 = "3-*-forward"
    s3 = "5-*-arrive"
    r1=0
    r2=0
    r3=0
    for str in arr:
        a = str.split(SEP)
        logtype = a[0]
        if logtype==s1:
            r1+=1
        elif logtype==s2:
            r2+=1
        elif logtype==s3:
            r3+=1
    rets+=[r1,r2,r3]

    return tuple(rets)




__author__ = 'kevin'

# drop
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
    day = "20160101"
    master = "local[*]"
    logger.info(day)
    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    #input = "/impala/parquet/back/back-portal-loginflowlog/dat=%s*" % day
    input = "/input/loginfowlog/part-r-00000-d4*"
    output = "/output"
    conf = (SparkConf()
            .setMaster(master)
            .setAppName("user_visit_week")
            #.set("spark.kryoserializer.buffer.mb", "256")
            .set("spark.sql.parquet.binaryAsString", "true"))
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    df = sqlContext.read.parquet(input)
    rdd = df.select('logintype', 'logtype', 'hosid', 'suppid', 'logtime', 'usermac')

    fields = [
        StructField('mac', StringType(), True),
        StructField('hosid', StringType(), True),
        StructField('year_week', StringType(), True),
        StructField('loginPage', IntegerType(), False),
        StructField('forwardPage', IntegerType(), False),
        StructField('arrivePage', IntegerType(), False)
    ]
    schema = StructType(fields)

    # compute pages
    rdd1 = rdd.map(convert_week)\
        .groupByKey().mapValues(list).map(convert_set)
    #rdd1.foreach(my_print)
    #(u'6C72E76890FC_1_2015-48', [u'1-prelogin_20151201', u'2-mobile-login_20151201'])
    rdd1_2 = rdd1.map(convert_visit_days)
    #rdd1_2.foreach(my_print)
    logger.info(rdd1_2.count())
    df1 =  sqlContext.createDataFrame(rdd1_2,schema)
    _output = output+"/mid_uservisitpage_week/%s" % day
    df1.coalesce(2).write.parquet(_output,'overwrite')

    sc.stop()