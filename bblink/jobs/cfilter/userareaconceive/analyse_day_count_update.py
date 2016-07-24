# -*- coding: utf-8 -*-
from operator import add

import os
import datetime
import random
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.types import StructField, IntegerType, StringType, StructType, LongType
from arithmetic_day_update import Arithmetic
#from bblink.jobs.cfilter.base_service import BaseService

__author__ = 'kevin'

SEP = u'-'
_SEP = "\t"

def print_str(str):
    print(str)


def myrandom(num):
    return int(random.random() * num)


# (mac_hosid,count)
def convert_hosid_count(k, v):
    arr = k.split(SEP)
    mac = arr[0]
    hosid = arr[1]
    # (mac,hosid-count)
    return (mac, hosid + SEP + str(v))


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
    max_v = 0
    max_k = ''
    for (k, v) in dic.items():
        if max_v < v:
            max_v = v
            max_k = k
    best_hosid = max_k

    return (mac, best_hosid)


def mill_day(timeStamp):
    mill = long(timeStamp / 1000)
    dateArray = datetime.datetime.utcfromtimestamp(mill)
    ret = dateArray.strftime("%Y-%m-%d")
    return ret


def convert_day(l):
    return (l[0], mill_day(long(l[2])))


def compute_mark(kvs):
    arith = Arithmetic()
    mac = kvs[0]
    set = list(kvs[1])
    ret = True
    # 用户怀孕时间
    conceive_time=u""
    # 用户身份标识
    mark = u'其他'
    size = len(set)
    # 5.首次登录：孕8月 or 孕前期
    if size == 1:
        mark = u'孕8月|孕前期'
        conceive_time = arith.daydiff(set[0],1)
        # print mac,mark
    elif size >= 2:
        # 0.每周连续:医护
        ret = arith.continue_perweek(set)
        if ret:
            mark = u'医护'
            # print mac,ret,mark
        # 1.每周登录大于3 & 夜间有上网记录:住院的病人
        # 每周登录大于3 & 夜间没有上网记录：不住院的病人
        ret = arith.perweek_morethan_3(set)
        if ret:
            mark = u'病人'
            # print mac,ret,mark

        # 2.每2周登录1次:孕8月
        ret = arith.per2weeks_equal_1(set)
        if ret:
            mark = u'孕8月'
            #print mac, ret, mark
            conceive_time = arith.daydiff(set[0],8)
        # 3.每1周登录1次&周数大于1:孕9月
        ret = arith.perweek_equal_1(set)
        if ret:
            mark = u'孕9月'
            # print mac,ret,mark
            conceive_time = arith.daydiff(set[0],9)
        # 4.每月(4周)登录1次:孕前期
        ret = arith.per4weeks_equal_1(set)
        if ret:
            mark = u'孕前期'
            # print mac,ret,mark
            conceive_time = arith.daydiff(set[0],1)

    # (mac,mark)
    return (mac, mark+"_"+conceive_time)

def generate_ret(r):
    #line = r[0]+_SEP+r[1][0]+_SEP+r[1][1]
    # join:(mac,(mark_conceive,hosid))
    a = r[1]
    ar=a[0].split("_")
    return (r[0],ar[0],ar[1],a[1])

# analyse (user,area,pregnant_period) base on day
# evaluate conceive time
if __name__ == '__main__':
    day = '20150731'

    master = "local[*]"
    app_name = "user_profile_app"

    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home
    # transfer_loginlog_extract
    input = "/output/back/back-portal-loginlog-parts/%s/*" % day
    output = "/impala/parquet/back/user-profile/dat=%s" % day

    conf = (SparkConf()
            .setMaster(master)
            .setAppName(app_name))

    sc = SparkContext(pyFiles=['arithmetic_day.py'], conf=conf)
    sqlContext = SQLContext(sc)

    # mac,user_name,login_time,gwid,hosid
    lines = sc.textFile(input)
    parts = lines.map(lambda l: l.split(_SEP)) \
        .filter(lambda l: len(l) == 5) \
        .filter(lambda l: len(l[2]) == 13)

    # user,hosid,count
    hosid_count = parts.map(lambda l: (l[0] + SEP + l[4], 1)).reduceByKey(add) \
        .map(lambda (k, v): convert_hosid_count(k, v))
    # hosid_count.foreach(print_str)

    user_top_hosid = hosid_count.groupByKey().mapValues(list).sortByKey() \
        .map(topcount)
    # (u'00:66:4B:9B:0F:C9', u'')
    # user_top_hosid.foreach(print_str)

    # user,days,count
    days_count = parts.map(convert_day).groupByKey().mapValues(set).map(compute_mark)
    #(u'95:15:DF:EE:41:E9', u'\u5b558\u6708|\u5b55\u524d\u671f', u'2014-04-06')

    #days_count.foreach(print_str)
    # join:mac,mark,hosid
    mac_mark_hosid = days_count.join(user_top_hosid).map(generate_ret)
    #mac_mark_hosid.take(10)
    #mac_mark_hosid.foreach(print_str)
    fields = [
        StructField('user', StringType(), True),
        StructField('stage', StringType(), True),
        StructField('conceive', StringType(), True),
        StructField('area', StringType(), True)
        ]
    schema = StructType(fields)
    dest = sqlContext.applySchema(mac_mark_hosid, schema)
    dest.registerTempTable("user_profile")
    # combine partition
    dest.coalesce(10).write.parquet(output,'overwrite')


    sc.stop()
