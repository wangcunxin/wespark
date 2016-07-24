# encoding: utf-8
from pyspark.sql.functions import UserDefinedFunction, countDistinct
from pyspark.sql.types import StructField, IntegerType, StringType, StructType, LongType
from bblink.back.dao import MysqlDao

__author__ = 'lidl'

import os
import sys

import datetime
import time

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
    return (str(x[0]).strip(), str(x[1]).strip(), str(x[2]).strip(), str(x[3]).strip(), long(x[4]), str(x[5]).strip())


def printx(x):
    print '---here---'
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
    if logtype == Logtype.prelogin:
        nums._prelogin = int(array[1])
        nums._preloginP = int(array[2])
    if logtype == Logtype.mobile_login:
        nums._mobile_login = int(array[1])
        nums._mobile_loginP = int(array[2])
    if logtype == Logtype.wechat_login:
        nums._wechat_login = int(array[1])
        nums._wechat_loginP = int(array[2])
    if logtype == Logtype.olduser_login:
        nums._olduser_login = int(array[1])
        nums._olduser_loginP = int(array[2])
    if logtype == Logtype.wechat_forward:
        nums._wechat_forward = int(array[1])
        nums._wechat_forwardP = int(array[2])
    if logtype == Logtype.mobile_forward:
        nums._mobile_forward = int(array[1])
        nums._mobile_forwardP = int(array[2])
    if logtype == Logtype.wechat_pre_arrive:
        nums._wechat_pre_arrive = int(array[1])
        nums._wechat_pre_arriveP = int(array[2])
    if logtype == Logtype.wechat_prearrive_authfail:
        nums._wechat_prearrive_authfail = int(array[1])
        nums._wechat_prearrive_authfailP = int(array[2])
    if logtype == Logtype.wechat_prearrive_authsuccess:
        nums._wechat_prearrive_authsuccess = int(array[1])
        nums._wechat_prearrive_authsuccessP = int(array[2])
    if logtype == Logtype.wechat_arrive:
        nums._wechat_arrive = int(array[1])
        nums._wechat_arriveP = int(array[2])
    if logtype == Logtype.mobie_arrive:
        nums._mobie_arrive = int(array[1])
        nums._mobie_arriveP = int(array[2])
    if logtype == Logtype.mobile_authfail_page:
        nums._mobile_authfail_page = int(array[1])
        nums._mobile_authfail_pageP = int(array[2])
    if logtype == Logtype.wechat_authfail_page:
        nums._wechat_authfail_page = int(array[1])
        nums._wechat_authfail_pageP = int(array[2])


class Logtype:
    prelogin = '1-prelogin'
    mobile_login = '2-mobile-login'
    wechat_login = '2-wechat-login'
    olduser_login = '2-olduser-login'
    wechat_forward = '3-wechat-forward'
    mobile_forward = '3-mobile-forward'

    wechat_prearrive_authfail = '4-wechat-prearrive-authfail'
    wechat_prearrive_authsuccess = '4-wechat-prearrive-authsuccess'
    wechat_arrive = '5-wechat-arrive'
    mobie_arrive = '5-mobile-arrive'
    mobile_authfail_page = '6-mobile-authfail-page'
    wechat_authfail_page = '6-wechat-authfail-page'

    wechat_open_url = '3-wechat-open-url'
    wechat_geturl = '3-wechat-geturl'
    log_wechat_auth = 'log-wechat-auth'
    #mobile_pre_arrive = '4-mobile-pre-arrive'
    wechat_arrive_url = '5-wechat-arrive-url'
    mobile_arrive_url = '5-mobile-arrive-url'

    # date
    wechat_login_click = "2-wechat-login-click"
    # success fail
    mobile_login_click = "2-mobile-login-click"

    mobile_pre_arrive = "4-mobile-pre-arrive"
    wechat_pre_arrive = "4-wechat-pre-arrive"

    # hosid,date
    wechat_forward_change_click = "3-wechat-forward-change-click"
    olduser_login_click = "2-olduser-login-click"


class Nums:
    def __init__(self):
        self._prelogin = 0
        self._preloginP = 0
        self._mobile_login = 0
        self._mobile_loginP = 0
        self._wechat_login = 0
        self._wechat_loginP = 0
        self._olduser_login = 0
        self._olduser_loginP = 0
        self._wechat_forward = 0
        self._wechat_forwardP = 0
        self._mobile_forward = 0
        self._mobile_forwardP = 0
        self._wechat_pre_arrive = 0
        self._wechat_pre_arriveP = 0
        self._wechat_prearrive_authfail = 0
        self._wechat_prearrive_authfailP = 0
        self._wechat_prearrive_authsuccess = 0
        self._wechat_prearrive_authsuccessP = 0
        self._wechat_arrive = 0
        self._wechat_arriveP = 0
        self._mobie_arrive = 0
        self._mobie_arriveP = 0
        self._mobile_authfail_page = 0
        self._mobile_authfail_pageP = 0
        self._wechat_authfail_page = 0
        self._wechat_authfail_pageP = 0

# drop
if __name__ == '__main__':

    if len(sys.argv) != 3:
        print("Usage: user_sign_in.py <input>")
        sys.exit(-1)

    date = sys.argv[1]
    master = sys.argv[2]

    spark_home = '/home/lidl/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    logFile = 'hdfs://master:8020/impala/parquet/back/back-portal-loginflowlog/dat=' + date

    conf = (SparkConf()
            .setMaster(master)
            .setAppName("loginflowlog2mysql")
            .set("spark.kryoserializer.buffer.mb", "256")
            .set("spark.sql.parquet.binaryAsString","true"))
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    sqlContext.registerFunction("todatestr", lambda x: longTime2str(x), StringType())
    sqlContext.registerFunction("trimx", lambda x: trimx(x), StringType())

    df = sqlContext.read.parquet(logFile)

    destDF=df.select('logintype','logtype','hosid','suppid','logtime','usermac').map(lambda x:trimf(x))
    fields = [
        StructField('logintype', StringType(), True),
        StructField('logtype', StringType(), True),
        StructField('hosid', StringType(), True),
        StructField('suppid', StringType(), True),
        StructField('logtime', LongType(), True),
        StructField('usermac', StringType(), True)
        ]
    schema = StructType(fields)
    schemaDest = sqlContext.applySchema(destDF, schema)
    schemaDest.registerTempTable("loginflowlog")


    _sql = "SELECT count(usermac) pv ,count(distinct usermac) uv,logtype,hosid,suppid," \
           "todatestr(logtime) day " \
           "from loginflowlog " \
           "group by logtype, hosid,suppid,todatestr(logtime)"
    midDF = sqlContext.sql(_sql)

    resultRDD = midDF.rdd.map(lambda x: (joinstr(x.hosid, x.suppid, x.day), joinstr(x.logtype, str(x.pv), str(x.uv)))) \
        .map(lambda x: (x[0], [x[1]])).reduceByKey(lambda a, b: a + b).v
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
        _tuple = (nums._prelogin, nums._preloginP,
                  nums._mobile_login, nums._mobile_loginP,
                  nums._wechat_login, nums._wechat_loginP,
                  nums._olduser_login, nums._olduser_loginP,
                  nums._wechat_forward, nums._wechat_forwardP,
                  nums._mobile_forward, nums._mobile_forwardP,
                  nums._wechat_pre_arrive, nums._wechat_pre_arriveP,
                  nums._wechat_prearrive_authfail, nums._wechat_prearrive_authfailP,
                  nums._wechat_prearrive_authsuccess, nums._wechat_prearrive_authsuccessP,
                  nums._wechat_arrive, nums._wechat_arriveP,
                  nums._mobie_arrive, nums._mobie_arriveP,
                  nums._mobile_authfail_page, nums._mobile_authfail_pageP,
                  nums._wechat_authfail_page, nums._wechat_authfail_pageP,
                  key[0], key[1], key[2])
        keysList.append(_keytuple)
        list.append(_tuple)

    dao = MysqlDao()
    dao.insertMany(
        'INSERT INTO `bblink_data`.`login_flow_count` (`hos_id`,`supp_id`,`date`)VALUES(%s,%s,%s) ON DUPLICATE KEY UPDATE hos_id=%s,supp_id=%s,date=%s',
        keysList)
    dao.insertMany("update `bblink_data`.`login_flow_count` "
                   " set"
                   "`prelogin_num`=%s, `prelogin_pnum`=%s, "
                   "`mobile_login_num`=%s, `mobile_login_pnum`=%s, "
                   "`wechat_login_num`=%s, `wechat_login_pnum`=%s, "
                   "`olduser_login_num`=%s, `olduser_login_pnum`=%s, "
                   "`wehcat_forward_num`=%s, `wehcat_forward_pnum`=%s,"
                   " `mobile_forward_num`=%s, `mobile_forward_pnum`=%s, "
                   "`wechat_pre_arrive_num`=%s, `wechat_pre_arrive_pnum`=%s, "
                   "`wechat_prearrive_authfail_num`=%s, `wechat_prearrive_authfail_pnum`=%s,"
                   " `wechat_prearrive_authsuccess_num`=%s, `wechat_prearrive_authsuccess_pnum`=%s, "
                   "`wechat_arrive_num`=%s, `wechat_arrive_pnum`=%s, "
                   "`mobile_arrive_num`=%s, `mobile_arrive_pnum`=%s, "
                   "`mobile_authfail_page_num`=%s, `mobile_authfail_page_pnum`=%s, "
                   "`wechat_authfail_page_num`=%s, `wechat_authfail_page_pnum`=%s where hos_id=%s and supp_id=%s and date=%s",
                   list)
