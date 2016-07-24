# -*- coding: utf-8 -*-

import sys

from bblink.jobs.transferdata.logger import *
from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType

__author__ = 'kevin'


def to_str(x):
    size = len(x)
    list = []
    for i in range(0, size):
        list.append(str(x[i]))
    return tuple(list)


def kws_contain(batch_no):
    ret = batch_no.find(u'nbblink_2015')
    return True if ret < 0 else False

def to_int(x):
    return 0 if x.strip()=='' else int(x)

def to_long(x):
    return 0L if x.strip()=='' else long(x)

def my_print(x):
    print(x)

# tranfer historical data to hdfs parquet table
if __name__ == '__main__':
    '''
    if len(sys.argv) != 2:
        print("Usage:transfer.py <date>")
        sys.exit(-1)

    day = sys.argv[1:]
    logger.info(day)
    '''
    #
    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    #master = "spark://hadoop:7077"
    master = "local[1]"
    app_name = "spark_transferdata"
    sep = "\t"
    #input = "/data/140301_150731.csv"
    input = "/input/loginlog/2015"
    output = "/output/loginlog/2015"

    sc = SparkContext(master, app_name)
    sqlContext = SQLContext(sc)
    # load
    lines = sc.textFile(input)
    rdd = lines.map(lambda l: l.split(sep))\
        .filter(lambda l:len(l)==11)\
        .map(lambda l:(l[0],l[1],l[2],to_long(l[3]),l[4],
                       long(l[5]),long(l[6]),l[7],l[8],l[9],
                       to_long(l[10])))
    # uid,adid,guuid,guuidctime,url,referer,hosid,gwid,ua,ip,createtime
    # uid,adid,guuid,createtime
    fields = [
        StructField('uid', StringType(), True),
        StructField('adid', StringType(), True),
        StructField('guuid', StringType(), True),
        StructField('guuidctime', LongType(), True),
        StructField('url', StringType(), True),

        StructField('referer', StringType(), True),
        StructField('hosid', StringType(), True),
        StructField('gwid', StringType(), True),
        StructField('ua', StringType(), True),
        StructField('ip', StringType(), True),

        StructField('createtime', LongType(), True),

    ]

    schema = StructType(fields)

    # [(),()] ['','']
    df_dest = sqlContext.createDataFrame(rdd, schema)
    df_dest.registerTempTable("back_portal_loginlog")

    #df_dest.rdd.foreach(my_print)
    # save
    df_dest.write.parquet(output)


    sc.stop()
