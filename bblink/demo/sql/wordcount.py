#-*- coding:utf8 -*-

from operator import add
import os

__author__ = 'kevin'

from pyspark import SparkContext, SparkConf

#中文测试
def my_print(l):
    print l

if __name__ == '__main__':
    master = "local[1]"
    app_name = "spark_sql_wc_app"
    input = 'hdfs://debian:8020/test/*'

    spark_home = '/home/kevin/galaxy/spark-1.6.2-bin-hadoop2.6'
    os.environ['SPARK_HOME'] = spark_home

    conf = (SparkConf()
            .setMaster(master)
            .setAppName(app_name))
            #.set("spark.executor.extraClassPath",'mysql-connector-java-5.1.18.jar')
            #.set('spark.io.compression.codec','snappy'))
    sc = SparkContext(conf=conf)

    #sum = sc.accumulator(0)
    #bcv = sc.broadcast([1,2,3])

    lines = sc.textFile(input)
    '''
    lines.foreach(my_print)
    sc.stop()
    '''
    word_count = lines.flatMap(lambda line: line.split("\t"))\
        .map(lambda word: (word, 1))\
        .reduceByKey(add)
    word_count.foreach(my_print)


    #print bcv.value
    sc.stop()


