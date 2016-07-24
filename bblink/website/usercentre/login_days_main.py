import time,datetime, sys

from pyspark.sql.types import StringType
from pyspark import SparkContext, SQLContext, SparkConf

from bblink.website.usercentre.logger import *
from bblink.website.usercentre.dao import MysqlDao

__author__ = 'kevin'

def normal_mac(mac):
    ar = mac.encode('utf-8')
    rets = None
    if len(ar)==12:
        ret = [ar[0]]
        for i in range(1,len(ar)):
            if i%2==0:
                ret.append(":")
            ret.append(ar[i])
        rets = ''.join(ret)
    else:
        rets = ar
    return rets

def to_timestamp(dat):
    t0 = str(long(time.mktime(time.strptime(dat, '%Y%m%d'))) * 1000)
    return t0

# calculate user login days per month
if __name__ == '__main__':
    # --set datetime
    DAY_OFFSET = 1
    now = datetime.datetime.now()
    pro_time = now - datetime.timedelta(days=DAY_OFFSET)
    ym = pro_time.strftime("%Y%m")
    ymd = pro_time.strftime("%Y%m%d")

    master = "spark://hadoop:7077"
    appName = "spark_loginflowlog"
    #input = "/impala/parquet/back/back-portal-loginflowlog/dat=%s*" % ym
    input = '/input/loginfowlog/*'

    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home
    conf = (SparkConf()
            .setMaster(master)
            .setAppName(appName)
            .set("spark.sql.parquet.binaryAsString","true")
            )
    sc = SparkContext(conf = conf)
    sql_context = SQLContext(sc)
    sql_context.registerFunction("to_mac", lambda x: normal_mac(x), StringType())

    parquet_df = sql_context.read.parquet(input)
    sql_context.registerDataFrameAsTable(parquet_df, "loginflowlog")
    #_sql = "select to_mac(upper(usermac)),count(distinct dat) days from loginflowlog group by to_mac(upper(usermac))"
    _sql = "select to_mac(upper(usermac)),count(distinct logtime) days from loginflowlog group by to_mac(upper(usermac))"
    rs_df = sql_context.sql(_sql)
    rs = rs_df.collect()
    logger.info("---->" + str(len(rs)))

    lists = []
    for r in rs:
        usermac = r[0]
        days = r[1]
        t = (usermac,days)
        lists.append(t)
        #logger.debug(t)

    dao = MysqlDao()

    _sql = "TRUNCATE TABLE user_days"
    dao.insert(_sql)

    _sql = "insert into user_days(user_mac,user_days) values(%s,%s);"
    dao.insertMany(_sql, lists)

    _sql = "SELECT b.user_id,a.user_mac,a.user_days FROM `user_days` a,`user` b WHERE a.user_mac=b.user_mac;"
    rs = dao.findWithQuery(_sql)

    # recongnize status:staff=0,patient=1
    lists = []
    create_time = to_timestamp(ymd)
    for r in rs:
        user_id = r[0]
        user_mac = r[1]
        user_days = r[2]
        t = None
        if int(user_days)<7:
            t=(user_id,user_mac,'1',create_time)
        else:
            t=(user_id,user_mac,'0',create_time)

        lists.append(t)
        #logger.debug(t)

    #update user_status
    _sql = "TRUNCATE TABLE user_status"
    dao.insert(_sql)
    _sql = "insert into user_status(user_id,user_mac,user_status,create_time) values(%s,%s,%s,%s);"
    dao.insertMany(_sql,lists)

    logger.info("---->finished")

    sc.stop()
