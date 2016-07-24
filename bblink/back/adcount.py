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


def longTime2str(longtime=0):
    if longtime is None:
	return '19700101'
    return datetime.datetime.fromtimestamp(long(longtime/1000)).strftime('%Y%m%d')
def printx(x):
    print(x)
def joinstr(*strs):
    return '_'.join(strs)
def fetchOne(x):
    return (x[0],iter(x[1]).next())
def convertNull(x):
    if x is None:
       x=0
    return x

if __name__ == '__main__':

    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    lastdate=''
    if len(sys.argv)==2:
	lastdate=sys.argv[1]
    else:
    	DAY_OFFSET=1
    	now =datetime.datetime.now();
    	pro_time=now-datetime.timedelta(days=DAY_OFFSET)
	lastdate=pro_time.strftime("%Y%m%d")
    print 'lastdate is :'+lastdate
    master = 'spark://master:7077'

    # logFile = 'hdfs://cdh-master:8020/impala/parquet/back/back-portal-loginflowlog/dat=20151124'
    adLoadFiles= 'hdfs://master:8020/impala/parquet/site/site-adLoadv1/dat='+lastdate
    adPlayFiles= 'hdfs://master:8020/impala/parquet/site/site-adPlayv1/dat='+lastdate
    adClickFiles= 'hdfs://master:8020/impala/parquet/site/site-adClickv1/dat='+lastdate

    conf = (SparkConf()
            .setMaster(master)
            .setAppName("adhoscount")
            .set("spark.kryoserializer.buffer.mb", "256")
            .set("spark.sql.parquet.binaryAsString","true")
            )
    sc = SparkContext(conf = conf)
    sqlContext = SQLContext(sc)

    _adloadDF=sqlContext.read.parquet(adLoadFiles)
    _adloadRdd=_adloadDF.rdd.map(lambda x:(x.guuid,x.hosid)).groupByKey().map(fetchOne)

    fields = [
        StructField('guuid', StringType(), True),
        StructField('hosid', StringType(), True),
        ]
    schema = StructType(fields)
    schemaDest = sqlContext.createDataFrame(_adloadRdd, schema)
    schemaDest.registerTempTable("ghid")

    _adloadDF.registerAsTable("adload")
    sqlContext.read.parquet(adPlayFiles).registerAsTable("adplay")
    sqlContext.read.parquet(adClickFiles).registerAsTable("adclick")

    '''
    _adLoadDF=sqlContext.createDataFrame([
        {'uid': '1', 'adid': 'a','guuid':'aa','guuidctime':1,'url':'','referer':'','hosid':'132','gwid':'','ua':'','ip':'','createtime':1450823568766},
        {'uid': '2', 'adid': 'b','guuid':'aa','guuidctime':1,'url':'','referer':'','hosid':'132','gwid':'','ua':'','ip':'','createtime':1450823569766},
        {'uid': '3', 'adid': 'c','guuid':'aa','guuidctime':1,'url':'','referer':'','hosid':'132','gwid':'','ua':'','ip':'','createtime':1450823550766},
        {'uid': '4', 'adid': 'd','guuid':'bb','guuidctime':1,'url':'','referer':'','hosid':'133','gwid':'','ua':'','ip':'','createtime':1450823268766},
    ]).registerAsTable("adload")
    _adPlayDF=sqlContext.createDataFrame([
        {'uid': '1', 'adid': 'a','guuid':'aa','createtime':1450823568766},
        {'uid': '2', 'adid': 'b','guuid':'aa','createtime':1450823569766},
        {'uid': '4', 'adid': 'd','guuid':'bb','createtime':1450823268766},
    ]).registerAsTable("adplay")
    _adClickDF =sqlContext.createDataFrame([
        {'uid': '1', 'adid': 'a','guuid':'aa','createtime':1450823580766},
    ]).registerAsTable("adclick")
    '''
    sqlContext.registerFunction("dateformat", lambda x:longTime2str(x),StringType())

    adLoadDf=sqlContext.sql('select hosid,dateformat(createtime) day,adid,count(guuid) pv,count(distinct guuid) uv '
                            'from adload where createtime is not null and dateformat(createtime)=%s '
                            'group by adid,hosid,dateformat(createtime)' % (lastdate)).registerAsTable("radload")

    adPlayDf=sqlContext.sql('select gh.hosid,dateformat(ap.createtime) day,adid,count(ap.guuid) pv,count(distinct ap.guuid) uv '
                            'from adplay ap left join ghid gh on ap.guuid=gh.guuid where dateformat(ap.createtime)=%s '
                            'group by ap.adid,gh.hosid,dateformat(ap.createtime)' % (lastdate)).registerAsTable("radplay")

    # sqlContext.sql('select sum(pv) from radplay').foreach(printx)
    adClick=sqlContext.sql('select gh.hosid,dateformat(ac.createtime) day,ac.adid,count(ac.guuid) pv,count(distinct ac.guuid) uv '
                            'from adclick ac left join ghid gh on ac.guuid=gh.guuid where dateformat(ac.createtime)=%s '
                            'group by ac.adid,gh.hosid,dateformat(ac.createtime)' % (lastdate)).registerAsTable("radclick")



    _df=sqlContext.sql('select A.hosid,A.day,A.adid,A.pv,A.uv,B.pv,B.uv,C.pv,C.uv from radload A '
                       'left join radplay B on A.hosid=B.hosid and A.adid=B.adid '
                       'left join radclick C on A.hosid=C.hosid and A.adid=C.adid')

    keysList=_df.rdd.map(lambda x:(x[0],x[1],x[2],x[0],x[1],x[2])).collect()
    resultList=_df.rdd.map(lambda x:(convertNull(x[3]),convertNull(x[4]),convertNull(x[5]),convertNull(x[6]),convertNull(x[7]),convertNull(x[8]),x[0],x[1],x[2])).collect()
    '''
    _df=sqlContext.sql("select dateformat(A.createtime) day,A.adid,count(A.guuid),count(distinct A.guuid),count(B.guuid),count(distinct B.guuid),count(C.guuid),count(distinct C.guuid) from adload A "
                       "left join adplay B on A.adid=B.adid and A.uid=B.uid and A.guuid=B.guuid "
                       "left join adclick C on A.adid=B.adid and A.uid=C.uid and A.guuid=C.guuid "
                       "where A.createtime is not null "
                       "group by A.adid,dateformat(A.createtime)")

    '''
    # _df.foreach(printx)


    dao=MysqlDao()
    dao.insertMany('INSERT INTO `bblink_data`.`bblink_platform_adv_hos_count` (`hos_id`,`day`,`advid`)VALUES(%s,%s,%s) '
                   'ON DUPLICATE KEY UPDATE hos_id=%s,day=%s,advid=%s',keysList)
    dao.insertMany("update `bblink_data`.`bblink_platform_adv_hos_count` "
                   "set "
                   "`load_pv`=%s,`load_uv`=%s,"
                   "`play_pv`=%s,`play_uv`=%s,"
                   "`click_pv`=%s,`click_uv`=%s "
                   "where hos_id=%s and day=%s and advid=%s ",resultList)

    adLoadDfSum=sqlContext.sql('select dateformat(createtime) day,adid,count(guuid) pv,count(distinct guuid) uv '
                               'from adload where createtime is not null and dateformat(createtime)=%s '
                               'group by adid,dateformat(createtime)' % (lastdate)).registerAsTable("radloadSum")

    adPlayDfSum=sqlContext.sql('select dateformat(createtime) day,adid,count(guuid) pv,count(distinct guuid) uv '
                            'from adplay where createtime is not null and dateformat(createtime)=%s '
                            'group by adid,dateformat(createtime)' % (lastdate)).registerAsTable("radplaySum")

    adClickSum=sqlContext.sql('select dateformat(createtime) day,adid,count(guuid) pv,count(distinct guuid) uv '
                            'from adclick where createtime is not null and dateformat(createtime)=%s '
                            'group by adid,dateformat(createtime)' % (lastdate)).registerAsTable("radclickSum")

    _dfSum=sqlContext.sql('select A.day day,A.adid advid,A.pv load_pv,A.uv load_uv,B.pv play_pv,B.uv play_uv,C.pv click_pv,C.uv click_uv from radloadSum A '
                       'left join radplaySum B on A.adid=B.adid '
                       'left join radclickSum C on A.adid=C.adid ')

    keysListSum=_dfSum.rdd.map(lambda x:(x[0],x[1],x[0],x[1])).collect()
    resultListSum=_dfSum.rdd.map(lambda x:(convertNull(x[2]),convertNull(x[3]),convertNull(x[4]),convertNull(x[5]),convertNull(x[6]),convertNull(x[7]),x[0],x[1])).collect()

    dao.insertMany('INSERT INTO `bblink_data`.`bblink_platform_adv_count` (`day`,`advid`)VALUES(%s,%s) '
                   'ON DUPLICATE KEY UPDATE day=%s,advid=%s',keysListSum)

    dao.insertMany("update `bblink_data`.`bblink_platform_adv_count` "
                   "set "
                   "`load_pv`=%s,`load_uv`=%s,"
                   "`play_pv`=%s,`play_uv`=%s,"
                   "`click_pv`=%s,`click_uv`=%s "
                   "where day=%s and advid=%s ",resultListSum)
    sc.stop()
