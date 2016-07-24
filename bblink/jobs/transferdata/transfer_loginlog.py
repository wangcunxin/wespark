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
    day = '20150731'

    master = "spark://master:7077"
    app_name = "spark_transferdata"
    sep = "\t"
    input = "file:///data/140301_150731.csv"
    output = "/impala/parquet/back/back-portal-loginlog/dat=%s" % day

    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    sc = SparkContext(master, app_name)
    sqlContext = SQLContext(sc)
    # load
    lines = sc.textFile(input)
    rdd = lines.map(lambda l: l.split(sep)) \
        .filter(lambda l: len(l) == 18) \
        .filter(lambda l: kws_contain(l[15])) \
        .map(lambda l: (l[0], l[1], l[2], l[3], l[4],
                        to_long(l[5]), to_long(l[6]), l[7], l[8], l[9],
                        to_int(l[10]), to_int(l[11]), l[12], l[13], l[14],
                        l[15], l[16], l[17]))

    fields = [
        StructField('portal', StringType(), True),
        StructField('id', StringType(), True),
        StructField('gw_id', StringType(), True),
        StructField('user_id', StringType(), True),
        StructField('user_name', StringType(), True),

        StructField('login_time', LongType(), True),
        StructField('logout_time', LongType(), True),
        StructField('mac', StringType(), True),
        StructField('ip', StringType(), True),
        StructField('user_agent', StringType(), True),

        StructField('download_flow', IntegerType(), True),
        StructField('upload_flow', IntegerType(), True),
        StructField('os', StringType(), True),
        StructField('browser', StringType(), True),
        StructField('ratio', StringType(), True),

        StructField('batch_no', StringType(), True),
        StructField('user_type', StringType(), True),
        StructField('supp_id', StringType(), True)
    ]

    schema = StructType(fields)

    # [(),()] ['','']
    df_dest = sqlContext.createDataFrame(rdd, schema)
    df_dest.registerTempTable("back_portal_loginlog")

    # df_dest.rdd.foreach(my_print)
    # save
    df_dest.write.parquet(output)

    sc.stop()
