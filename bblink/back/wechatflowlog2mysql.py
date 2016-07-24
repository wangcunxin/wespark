# encoding: utf-8
from pyspark.sql.functions import UserDefinedFunction, countDistinct
from pyspark.sql.types import StructField, IntegerType, StringType, StructType, LongType

__author__ = 'lidl'

import os
import sys

import datetime
import time
from bblink.back.dao import MysqlDao
from pyspark import SparkContext, SparkConf, SQLContext

reload(sys)
sys.setdefaultencoding('utf8')


def dateConvLongtime(date):
    if date != None and date != '' and date != 'None':
        s = time.mktime(time.strptime(date, "%Y-%m-%s %H:%M:%S"))
        return s
    else:
        return getTime_format()


def getTime_format():
    return datetime.datetime.strptime("1970-01-01 00:00:00", "%Y-%m-%s %H:%M:%S")


def longTime2str(longtime=0):
    return datetime.datetime.fromtimestamp(long(longtime / 1000)).strftime('%Y%m%d')


def mapf(x):
    return (x[0], x[1], x[2], x[3], x[4])


def trimf(x):
    return (str(x[0]).strip(), str(x[1]).strip(), str(x[2]).strip(), long(x[3]), str(x[4]).strip())


def printx(x):
    print(x)


def trimx(x):
    return x.strip()


def joinstr(*strs):
    return '_'.join(strs)


def values2row(x):
    _list = []
    _list.append(x)
    return _list


def valuesnum(nums=None, subvalue=''):
    array = subvalue.split('_')
    logtype = array[0]
    if logtype == Logtype.getUrl:
        nums._geturl_num = int(array[1])
        nums._geturl_pnum = int(array[2])
    if logtype == Logtype.authEmpty:
        nums._authempty_num = int(array[1])
        nums._authempty_pnum = int(array[2])
    if logtype == Logtype.accessUrl:
        nums._accessurl_num = int(array[1])
        nums._accessurl_pnum = int(array[2])
    if logtype == Logtype.accessUrlWechatSuccess:
        nums._accessurl_wechatsuccess_num = int(array[1])
        nums._accessurl_wechatsuccess_pnum = int(array[2])
    if logtype == Logtype.getApi:
        nums._getapi_num = int(array[1])
        nums._getapi_pnum = int(array[2])
    if logtype == Logtype.getApiParamEmpty:
        nums._getapi_param_empty_num = int(array[1])
        nums._getapi_param_empty_pnum = int(array[2])
    if logtype == Logtype.apiSuccess:
        nums._apisuccess_num = int(array[1])
        nums._apisuccess_pnum = int(array[2])
    if logtype == Logtype.apiFail:
        nums._apifall_num = int(array[1])
        nums._apifall_pnum = int(array[2])
    if logtype == Logtype.wechatSuccess:
        nums._wechatsuccess_num = int(array[1])
        nums._wechatsuccess_pnum = int(array[2])
    if logtype == Logtype.wechatFail:
        nums._wechatfall_num = int(array[1])
        nums._wechatfall_pnum = int(array[2])

#drop
if __name__ == '__main__':

    if len(sys.argv) != 3:
        print("Usage: user_sign_in.py <input>")
        sys.exit(-1)

    date = sys.argv[1]
    master = sys.argv[2]

    spark_home = '/home/lidl/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    logFile = 'hdfs://master:8020//impala/parquet/back/back-portal-wechatflowlog/dat=%s' % date

    conf = (SparkConf()
            .setMaster(master)
            .setAppName("wechatflowlog2mysql")
            .set("spark.kryoserializer.buffer.mb", "256"))
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    df = sqlContext.read.parquet(logFile)
    print df.schema
    destDF = df.select('type', 'hosid', 'suppid', 'logtime', 'usermac').map(lambda x: trimf(x))
    fields = [
        StructField('type', StringType(), True),
        StructField('hosid', StringType(), True),
        StructField('suppid', StringType(), True),
        StructField('logtime', LongType(), True),
        StructField('usermac', StringType(), True)
    ]
    schema = StructType(fields)
    schemaDest = sqlContext.applySchema(destDF, schema)
    schemaDest.registerTempTable("wechatflowlog")

    sqlContext.registerFunction("todatestr", lambda x: longTime2str(x), StringType())
    sqlContext.registerFunction("trimx", lambda x: trimx(x), StringType())
    midDF = sqlContext.sql(
        "SELECT count(1) pv ,count(distinct usermac) uv,type,hosid,suppid,todatestr(logtime) day from wechatflowlog "
        "where usermac!='' group by type, hosid,suppid,todatestr(logtime)")


    class Logtype:
        getUrl = '1-GETURL'
        authEmpty = '1-AUTHEMPTY'
        accessUrl = '2-ACCESSURL'
        accessUrlWechatSuccess = '2-ACCESSURL WECHATSUCCESS'
        getApi = '3-GETAPI'
        getApiParamEmpty = '3-GETAPI-PARAM-EMPTY'
        apiSuccess = '4-APISUCCESS'
        apiFail = '5-APIFAIL'
        wechatSuccess = '6-WECHATSUCCESS'
        wechatFail = '7-WECHATFAIL'


    class Nums:
        def __init__(self):
            self._geturl_num = 0
            self._geturl_pnum = 0
            self._authempty_num = 0
            self._authempty_pnum = 0
            self._accessurl_num = 0
            self._accessurl_pnum = 0
            self._accessurl_wechatsuccess_num = 0
            self._accessurl_wechatsuccess_pnum = 0
            self._getapi_num = 0
            self._getapi_pnum = 0
            self._getapi_param_empty_num = 0
            self._getapi_param_empty_pnum = 0
            self._apisuccess_num = 0
            self._apisuccess_pnum = 0
            self._apifall_num = 0
            self._apifall_pnum = 0
            self._wechatsuccess_num = 0
            self._wechatsuccess_pnum = 0
            self._wechatfall_num = 0
            self._wechatfall_pnum = 0


    resultRDD = midDF.rdd.map(lambda x: (joinstr(x.hosid, x.suppid, x.day), joinstr(x.type, str(x.pv), str(x.uv)))).map(
        lambda x: (x[0], [x[1]])).reduceByKey(lambda a, b: a + b)
    templist = resultRDD.collect()
    list = []
    keysList = []
    for item in templist:
        key = item[0].split('_')
        values = item[1]
        nums = Nums()
        for subvalue in iter(values):
            valuesnum(nums, subvalue)
        _keytuple = (key[0], key[1], key[2], key[0], key[1], key[2])
        _tuple = (
            nums._geturl_num, nums._geturl_pnum,
            nums._authempty_num, nums._authempty_pnum,
            nums._accessurl_num, nums._accessurl_pnum,
            nums._accessurl_wechatsuccess_num, nums._accessurl_wechatsuccess_pnum,
            nums._getapi_num, nums._getapi_pnum,
            nums._getapi_param_empty_num, nums._getapi_param_empty_pnum,
            nums._apisuccess_num, nums._apisuccess_pnum,
            nums._apifall_num, nums._apifall_pnum,
            nums._wechatsuccess_num, nums._wechatsuccess_pnum,
            nums._wechatfall_num, nums._wechatfall_pnum,
            key[0], key[1], key[2]
        )
        print(_tuple)
        keysList.append(_keytuple)
        list.append(_tuple)

    dao = MysqlDao()
    dao.insertMany(
        'INSERT INTO `bblink_data`.`login_wechat_flow_count` (`hos_id`,`supp_id`,`date`)VALUES(%s,%s,%s) ON DUPLICATE KEY UPDATE hos_id=%s,supp_id=%s,date=%s',
        keysList)
    dao.insertMany("update `bblink_data`.`login_wechat_flow_count` "
                   " set"
                   " `geturl_num`=%s, `geturl_pnum`=%s,"
                   "`authempty_num`=%s, `authempty_pnum`=%s,"
                   " `accessurl_num`=%s, `accessurl_pnum`=%s,"
                   " `accessurl_wechatsuccess_num`=%s, `accessurl_wechatsuccess_pnum`=%s, "
                   "`getapi_num`=%s, `getapi_pnum`=%s, "
                   "`getapi_param_empty_num`=%s, `getapi_param_empty_pnum`=%s,"
                   " `apisuccess_num`=%s, `apisuccess_pnum`=%s, "
                   "`apifall_num`=%s, `apifall_pnum`=%s,"
                   " `wechatsuccess_num`=%s, `wechatsuccess_pnum`=%s, "
                   "`wechatfall_num`=%s, `wechatfall_pnum`=%s where hos_id=%s and supp_id=%s and date=%s", list)
