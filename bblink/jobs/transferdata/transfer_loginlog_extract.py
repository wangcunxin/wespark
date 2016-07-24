import os,sys

from pyspark import SQLContext, SparkContext
from pyspark.sql.types import *


from logger import *
from dao import MysqlDao

__author__ = 'kevin'

def myprint(l):
    print(l)

# owing to different old and new login_log ,so extract some fields
if __name__ == '__main__':
    '''
    1.select
    2.according to gwid,and get hosid
    3.save
    '''
    sep ="\t"
    master = "spark://master:7077"
    day = "20150731"
    #input = "/impala/parquet/back/back-portal-loginlog/dat=%s" % day
    input = "/impala/parquet/back/back-portal-loginflowlog/dat=%s*" % day[0:6]
    #input = "/input/loginlog/20150731"
    output = "/output/back/back-portal-loginlog-parts/%s" % day
    '''
    # 2
    _sql = "SELECT t.`hospital_id`,t.`gw_id`,t.`supplier_id` FROM bblink_wifi_info t WHERE t.`hospital_id` IS NOT NULL and gw_is_work='Y' and gw_is_change='N' ORDER BY t.`hospital_id`"
    dao = MysqlDao()
    rs_tuples = dao.findWithQuery(_sql)
    dic = {}
    for r in rs_tuples:
        hos_id = str(r[0]).decode('utf-8')
        gw_id = r[1].upper().decode('utf-8')
        dic[gw_id] = hos_id
    logger.debug('-->gwid_hosid:' + str(dic.__len__()))
    '''
    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    sc = SparkContext(master, 'transfer_login_old_app')
    sql_context = SQLContext(sc)
    # 1
    parquet_df = sql_context.read.parquet(input)

    # mac,user_name,login_time,gwid,hosid
    # old:back_portal_loginlog
    #rdd = parquet_df.rdd.map(lambda l:(None,l[7].upper()+sep+l[4]+sep+str(l[5]).decode('utf-8')+sep+l[2]+sep+u'')).values()
    # new:back_portal_loginflowlog
    rdd = parquet_df.rdd.map(lambda l:(None,l[5].upper()+sep+u''+sep+str(l[11]).decode('utf-8')+sep+l[4]+sep+l[2]))\
        .values().repartition(10)

    #rdd.foreach(myprint)
    rdd.saveAsTextFile(output)

    logger.info("complete to handle")

    sc.stop()