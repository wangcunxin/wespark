# encoding: utf-8
from pyspark.sql.functions import UserDefinedFunction, countDistinct
from pyspark.sql.types import StructField, IntegerType, StringType, StructType, LongType

import os
import sys

import datetime
import time
from bblink.back.dao import MysqlDao
from pyspark import SparkContext,SparkConf, SQLContext


reload(sys)
sys.setdefaultencoding('utf8')


def dateConvLongtime( date):
    if date != None and date != '' and date != 'None':
        s = time.mktime(time.strptime(date, "%Y-%m-%s %H:%M:%S"))
        return s
    else:
        return getTime_format()

def getTime_format():
    return datetime.datetime.strptime("1970-01-01 00:00:00", "%Y-%m-%s %H:%M:%S")
def longTime2str(longtime=0):
    return datetime.datetime.fromtimestamp(long(longtime/1000)).strftime('%Y%m%d')

def mapf(x):
    return (x[0],x[1],x[2],x[3],x[4])
def trimf(x):
    try:
    	return (str(x[0]).strip(),str(x[1]).strip(),str(x[2]).strip(),str(x[3]).strip(),long(x[4]),str(x[5]).strip(),str(x[6]).strip())
    except:
	return x;
def printx(x):
    print(x)
def trimx(x):
    return x.strip()
def joinstr(*strs):
    return '_'.join(strs)
def values2row(x):
    _list=[]
    _list.append(x)
    return _list


if __name__ == '__main__':

    if len(sys.argv) != 3:
        print("Usage: user_sign_in.py <input>")
        sys.exit(-1)

    date = sys.argv[1]
    master = sys.argv[2]

    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    logFile = 'hdfs://master:8020/impala/parquet/back/back-portal-loginflowlog/dat='+date

    conf = (SparkConf()
         .setMaster(master)
         .setAppName("hossubject_userpvuv")
         .set("spark.kryoserializer.buffer.mb", "256"))


    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)

    df = sqlContext.read.parquet(logFile)
    destDF=df.select('logintype','logtype','hosid','suppid','logtime','usermac','gwid').map(lambda x:trimf(x))
    fields = [
        StructField('logintype', StringType(), True),
        StructField('logtype', StringType(), True),
        StructField('hosid', StringType(), True),
        StructField('suppid', StringType(), True),
        StructField('logtime', LongType(), True),
        StructField('usermac', StringType(), True),
        StructField('gwid', StringType(), True)
        ]
    schema = StructType(fields)
    schemaDest = sqlContext.createDataFrame(destDF, schema)
    schemaDest.registerTempTable("loginflowlog")

    sqlContext.registerFunction("todatestr", lambda x:longTime2str(x),StringType())
    sqlContext.registerFunction("trimx", lambda x:trimx(x),StringType())
    midDF = sqlContext.sql("select count(1) userlogintimes,count(distinct(usermac)) userlogincount,hosid,gwid,todatestr(logtime) day from loginflowlog "
                           "where logtype like '5-%-arrive' and gwid!='' group by hosid,gwid,todatestr(logtime)")

    hosiddayList=midDF.rdd.map(lambda x:(x[2],x[3],x[4],x[2],x[3],x[4])).collect()
    resultList=midDF.rdd.collect()

    dao=MysqlDao()
    dao.insertMany('INSERT INTO `bblink_data`.`bblink_data_hos_subject` (`hosid`,`gwid`,`day`)VALUES(%s,%s,%s) ON DUPLICATE KEY UPDATE hosid=%s,gwid=%s,day=%s',hosiddayList)


    dao.insertMany("update `bblink_data`.`bblink_data_hos_subject` set userlogintimes=%s,userlogincount=%s where hosid=%s and gwid=%s and day=%s",resultList);

