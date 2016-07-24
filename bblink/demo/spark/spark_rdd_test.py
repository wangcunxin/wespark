# encoding: utf-8

import os
import datetime
from pyspark import SparkContext, SparkConf, SQLContext

__author__ = 'kevin'

# <user,week,count>
sep = '-'


def print_tuple(k, v):
    print(k, v)


def print_str(str):
    print(str)


def fun1(k, vs):
    dic = {}
    for v in vs:
        a = v.split("_")
        key = a[0]
        value = a[1]
        if dic.has_key(key):
            dic[key].append(value)
        else:
            dic[key] = [value]

    list = []
    for w in dic.keys():
        list.append(w + sep + str(len(set(dic[w]))))

    return (k, list)


def mill_date_week(timeStamp):
    mill = long(timeStamp / 1000)
    dateArray = datetime.datetime.utcfromtimestamp(mill)
    ret = dateArray.strftime("%Y-%W_%Y%m%d")
    return ret


def convert_week(l):
    year_week = mill_date_week(long(l[2]))
    return (l[0], year_week)


if __name__ == '__main__':
    _sep = "\t"
    master = "local[*]"
    app_name = "spark_rdd"

    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    conf = (SparkConf()
            .setMaster(master)
            .setAppName(app_name))
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    '''
    sample = [('mac01','20151201'),('mac01','20151202'),
              ('mac02','20151203'),('mac01','20151203'),
              ('mac03','20151203'),('mac04','20151205')]

    lines=sc.parallelize(sample,1)
    rdd1 = lines.groupByKey().mapValues(list).sortByKey().map(lambda (k,v):(k,len(v)))
    rdd1.foreach(print_tuple)
    '''

    sample = [('mac01', 'w1_20151201'), ('mac01', 'w1_20151202'),
              ('mac02', 'w2_20151203'), ('mac01', 'w2_20151203'),
              ('mac02', 'w2_20151204'), ('mac01', 'w2_20151203'),
              ('mac02', 'w3_20151203'), ('mac01', 'w2_20151203'),
              ('mac03', 'w4_20151203'), ('mac04', 'w3_20151205')]
    lines = sc.parallelize(sample, 1)
    rdd1 = lines.groupByKey().mapValues(list).sortByKey().map(lambda (k, vs): fun1(k, vs))
    rdd1.foreach(print_str)

    sc.stop()
