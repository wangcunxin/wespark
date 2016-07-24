# -*- coding: utf-8 -*-

import os, sys, datetime, time

from pyspark.sql.functions import UserDefinedFunction, countDistinct
from pyspark.sql.types import StructField, IntegerType, StringType, StructType, LongType
from pyspark import SparkContext, SparkConf, SQLContext
from bblink.back.logger import *

from bblink.back.dao import MysqlDao
from bblink.back.loginflowlog_service import LoginflowlogMysqlService


def longTime2str(longtime=0):
    return datetime.datetime.fromtimestamp(long(longtime / 1000)).strftime('%Y%m%d')


def printx(x):
    print(x)


def joinstr(*strs):
    return '_'.join(strs)


def convert_logtype(r):
    # 'logintype','logtype','hosid','suppid','logtime','usermac'
    _logtype = r[1].strip()
    tup = None
    s1 = "1-prelogin"
    s2 = "2-wechat-login,2-mobile-login,2-olduser-login"
    s3 = "2-wechat-login-click,2-mobile-login-click-success,2-mobile-login-click-fail,2-olduser-login-click"
    s4 = "3-wechat-forward,3-mobile-forward"
    s5 = "4-wechat-pre-arrive,4-mobile-pre-arrive"
    s6 = "5-wechat-arrive,5-mobile-arrive"
    ret = _logtype
    if s1.find(_logtype) != -1:
        ret = s1
    elif s2.find(_logtype) != -1:
        ret = '2-*-login'
    elif s3.find(_logtype) != -1:
        ret = '2-*-login-click'
    elif s4.find(_logtype) != -1:
        ret = '3-*-forward'
    elif s5.find(_logtype) != -1:
        ret = '4-*-pre-arrive'
    elif s6.find(_logtype) != -1:
        ret = '5-*-arrive'

    try:
        tup = (str(r[0]).strip(), ret, str(r[2]), str(r[3]), long(str(r[4]).strip()), str(r[5]).strip().upper())
    except Exception, e:
        print r
        print e

    return tup


def convert_copy_logtype(r):
    # ('1448931432525', [('WECHAT', '5-wechat-arrive', '635', '3', 1448931432525L, '')])

    a = r[1][0]

    # 'logintype','logtype','hosid','suppid','logtime','usermac'
    _logtype = a[1]

    s1 = "2-mobile-login-click-success,2-mobile-login-click-fail"
    s2 = "2-wechat-login,2-mobile-login,2-olduser-login"
    s3 = "2-mobile-login,2-olduser-login"
    # s4 = "2-mobile-login-click-success,2-mobile-login-click-fail,2-olduser-login-click"

    ret = _logtype
    ret_list = r[1]

    # combine success and fail
    if (s1.find(_logtype) != -1) and (_logtype.find("-click-") != -1):
        ret = '2-*-mobile-login-click'
        ret_list.append((a[0], ret, a[2], a[3], a[4], a[5]))
    if s2.find(_logtype) != -1:
        ret = '2-*-login'
        ret_list.append((a[0], ret, a[2], a[3], a[4], a[5]))
    if s3.find(_logtype) != -1:
        ret = '2-*-mobile-login'
        ret_list.append((a[0], ret, a[2], a[3], a[4], a[5]))

    return (r[0], ret_list)


def convert_list(r):
    ret = None
    try:
        t = (
        str(r[0]).strip(), str(r[1]).strip(), str(r[2]), str(r[3]), long(str(r[4]).strip()), str(r[5]).strip().upper())
        ret = (str(r[4]).strip(), [t])
    except:
        pass
    return ret


# eagle2:overall(wechat,mobile),wechat,mobile
if __name__ == '__main__':
    '''
    if len(sys.argv) != 3:
        print("Usage: user_sign_in.py <input>")
        sys.exit(-1)

    date = sys.argv[1]
    master = sys.argv[2]
    '''
    day = "20151212"
    master = "local[*]"

    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    # logFile = 'hdfs://master:8020/impala/parquet/back/back-portal-loginflowlog/dat=' + day
    logFile = "/input/loginfowlog/02*"
    conf = (SparkConf()
            .setMaster(master)
            .setAppName("loginflowlog2mysql")
            # .set("spark.kryoserializer.buffer.mb", "256")
            .set("spark.sql.parquet.binaryAsString", "true"))
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    sqlContext.registerFunction("to_datestr", lambda x: longTime2str(x), StringType())

    df = sqlContext.read.parquet(logFile)

    rdd = df.select('logintype', 'logtype', 'hosid', 'suppid', 'logtime', 'usermac')

    fields = [
        StructField('logintype', StringType(), True),
        StructField('logtype', StringType(), True),
        StructField('hosid', StringType(), True),
        StructField('suppid', StringType(), True),
        StructField('logtime', LongType(), True),
        StructField('usermac', StringType(), True)
    ]
    schema = StructType(fields)

    rdd1 = rdd.map(convert_logtype).filter(lambda tup: tup != None)
    # rdd1.foreach(printx)
    # sc.stop()

    ret_df = sqlContext.createDataFrame(rdd1, schema)
    ret_df.registerTempTable("loginflowlog_overall")
    _sql = "SELECT count(usermac) pv,count(distinct usermac) uv,logtype " \
           "from loginflowlog_overall " \
           "group by logtype"
    rs_df = sqlContext.sql(_sql)

    service = LoginflowlogMysqlService()
    ret_overall_list = service.getRetOverall(rs_df.collect(), day)
    _sql_delete = "delete from login_flow_global_count where date ='%s'" % day
    _sql_insert = "insert into login_flow_global_count(date," \
                  "prelogin_num,prelogin_pnum,login_num,login_pnum," \
                  "login_click_num,login_click_pnum,forward_num,forward_pnum," \
                  "preArrive_num,preArrive_pnum,arrive_num,arrive_pnum) " \
                  "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    service.write_mysql(ret_overall_list, _sql_delete, _sql_insert)
    logger.info(len(ret_overall_list))

    # detail
    rdd2 = rdd.map(convert_list).filter(lambda tup: tup != None) \
        .map(convert_copy_logtype).values().flatMap(list)
    # rdd2.foreach(printx)
    ret_df = sqlContext.createDataFrame(rdd2, schema)

    ret_df.registerTempTable("loginflowlog_detail")
    _sql = "SELECT count(usermac) pv,count(distinct usermac) uv,logtype,hosid,suppid,%s day " \
           "from loginflowlog_detail " \
           "group by logtype, hosid,suppid" % day
    rs2_df = sqlContext.sql(_sql)

    resultRDD = rs2_df.rdd.map(
        lambda x: (joinstr(x.hosid, x.suppid, str(x.day)), joinstr(x.logtype, str(x.pv), str(x.uv)))) \
        .map(lambda x: (x[0], [x[1]])).reduceByKey(lambda a, b: a + b)

    # (u'663_3_20151212', [u'5-wechat-arrive_2706_1', u'4-wechat-pre-arrive_1624_1'])
    rs_list = resultRDD.collect()

    service = LoginflowlogMysqlService()
    ret_list_keys = service.getRetKeys(rs_list)
    ret_list_values = service.getRetValues(rs_list)
    insert_sql = 'INSERT INTO `bblink_data`.`login_flow_count` (`hos_id`,`supp_id`,`date`)VALUES(%s,%s,%s) ON DUPLICATE KEY UPDATE hos_id=%s,supp_id=%s,date=%s'
    update_sql = "update `bblink_data`.`login_flow_count` " \
                 "set " \
                 "`prelogin_num`=%s, `prelogin_pnum`=%s, " \
                 "`mobile_login_num`=%s, `mobile_login_pnum`=%s, " \
                 "`wechat_login_num`=%s, `wechat_login_pnum`=%s, " \
                 "`olduser_login_num`=%s, `olduser_login_pnum`=%s, " \
                 "`wehcat_forward_num`=%s, `wehcat_forward_pnum`=%s," \
                 "`mobile_forward_num`=%s, `mobile_forward_pnum`=%s, " \
                 "`wechat_pre_arrive_num`=%s, `wechat_pre_arrive_pnum`=%s, " \
                 "`wechat_prearrive_authfail_num`=%s, `wechat_prearrive_authfail_pnum`=%s," \
                 "`wechat_prearrive_authsuccess_num`=%s, `wechat_prearrive_authsuccess_pnum`=%s, " \
                 "`wechat_arrive_num`=%s, `wechat_arrive_pnum`=%s, " \
                 "`mobile_arrive_num`=%s, `mobile_arrive_pnum`=%s, " \
                 "`mobile_authfail_page_num`=%s, `mobile_authfail_page_pnum`=%s, " \
                 "`wechat_authfail_page_num`=%s, `wechat_authfail_page_pnum`=%s," \
                 "`wechat_login_click_num`=%s, `wechat_login_click_pnum`=%s," \
                 "`wechat_forward_change_click_num`=%s, `wechat_forward_change_click_pnum`=%s," \
                 "`wechat_authurl_num`=%s, `wechat_authurl_pnum`=%s," \
                 "`wechat_pre_arrive_num`=%s, `wechat_pre_arrive_pnum`=%s," \
                 "`mobile_login_click_num`=%s, `mobile_login_click_pnum`=%s," \
                 "`olduser_login_click_num`=%s, `olduser_login_click_pnum`=%s," \
                 "`mobile_pre_arrive_num`=%s, `mobile_pre_arrive_pnum`=%s," \
                 "`mobile_login_click_success_num`=%s, `mobile_login_click_success_pnum`=%s," \
                 "`login_num`=%s, `login_pnum`=%s," \
                 "`all_mobile_login_num`=%s, `all_mobile_login_pnum`=%s" \
                 " where hos_id=%s and supp_id=%s and date=%s"

    service.write_mysql_insert_update(insert_sql, ret_list_keys, update_sql, ret_list_values)
    logger.info(len(ret_list_values))

    sc.stop()
