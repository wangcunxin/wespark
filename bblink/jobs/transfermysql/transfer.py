import os,sys

from pyspark import SQLContext, SparkContext
from pyspark.sql.types import *


from bblink.jobs.transfermysql.logger import *
from bblink.jobs.transfermysql.dao import MysqlDao

__author__ = 'kevin'

'''
    mysql -h114.215.197.160 -uw_bblink_hos -plDRejp6zMS5JpaLNE -D bblink_hos
    mysql -h121.40.175.62 -ubblink_data -pZTpDbEjpNI0vx32cFSft -Dbblink_data

    MYSQL_HOST='114.215.197.160'
    MYSQL_PORT=3306
    MYSQL_USERNAME='w_bblink_hos'
    MYSQL_PASSWD='lDRejp6zMS5JpaLNE'
    MYSQL_DB='bblink_hos'

'''

if __name__ == '__main__':
    '''
    1.select
    2.according to gwid,and get hosid
    3.insert
    4.export
    '''
    sep =","
    master = "spark://hadoop:7077"
    input = "hdfs://hadoop:9000/input/userinfo/bblink_wxcity_count_info.csv"

    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    sc = SparkContext(master, 'wxcity_userinfo__app')
    sql_context = SQLContext(sc)
    # 1
    lines = sc.textFile(input)
    rdd = lines.map(lambda line:line.replace("\"",'')).map(lambda line:line.split(sep)).filter(lambda l:len(l)==13)

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

    # hosid,uv,pv,day,gwid
    rdd2 = rdd.map(lambda l:(dic.get(l[0].upper(),u''),l[3],l[2],l[1].replace('-',''),l[0].upper()))

    map = {}
    for r in rdd2.collect():
        value = (r[0],r[1],r[2],r[3],r[4])
        key = r[0]+u"_"+r[3]+u"_"+r[4]
        map[key] = value

    list = map.values()
    logger.info(len(list))
    # 3
    if len(list)>0:
        insert_sql = "insert into bblink_data_hos_subject(hosid,userlogincount,userlogintimes,day,gwid) values(%s,%s,%s,%s,%s)"
        dao.insertMany(insert_sql,list)

    sc.stop()