# -*- coding: utf-8 -*-

import sys

from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StructField, StructType, StringType
from bblink.jobs.cfilter.logger import *
from bblink.jobs.cfilter.base_service import BaseService

__author__ = 'kevin'

# <user,date,sign> :<mac001,20151111,1/0>
if __name__ == '__main__':

    '''
    /impala/txt/back/back-portal-loginlog/dat=20151113
    /output/back//back-portal-loginlog/20151112
    '''
    if len(sys.argv) != 2:
        print("Usage: user_sign_in.py <input>")
        sys.exit(-1)

    date = sys.argv[1]
    '''
    input = "/input/userareaconceive/user_login_info.log"
    output="/home/kevin/works/logs/%s" % date
    '''
    input = "/impala/txt/back/back-portal-loginlog/dat=%s" % date
    output="/root/tmp/uservisitrecord/%s" % date

    logger.debug(date,input,output)
    '''
    DAY_OFFSET=1
    #--set datetime
    now =datetime.datetime.now()
    pro_time=now-datetime.timedelta(days=DAY_OFFSET)
    dest_time_str=pro_time.strftime("%Y%m%d")
    '''
    master = "spark://master:7077"
    sep = "\t"
    app_name = 'user_sign_in_app'
    '''
    spark_home='/opt/cloud/spark'
    os.environ['SPARK_HOME']=spark_home
    '''
    sc = SparkContext(master, app_name)
    sql_context = SQLContext(sc)

    lines = sc.textFile(input)
    parts = lines.map(lambda l: l.split(sep)).filter(lambda x: len(x) == 18)
    '''
    portal id(*) gw_id user_id user_name
    login_time logout_time(*) mac ip user_agent
    download_flow(*) upload_flow(*) os browser ratio
    batch_no user_type supp_id
    '''
    user_login = parts.map(lambda p: (p[1].strip(), p[2].strip(),p[17].strip(),p[3].strip(),p[16].strip(),
                                  p[4].strip(),p[5].strip(),p[6].strip(),p[7].strip(),p[8].strip(),
                                  p[9].strip(),p[10].strip(),p[11].strip(),p[12].strip(),p[13].strip(),
                                  p[14].strip(),p[15].strip()))
    schema_string = "id gw_id supp_id user_id user_type " \
                   "user_name login_time logout_time mac ip " \
                   "user_agent download_flow upload_flow os browser " \
                   "ratio batch_no"

    fields = [StructField(field_name, StringType(), True) for field_name in schema_string.split(' ')]
    schema = StructType(fields)

    df = sql_context.createDataFrame(user_login, schema)
    df.registerTempTable("tb_user_login_info")

    #_sql="select distinct mac,gw_id,'%s' as day,'1' as flag from tb_user_login_info" % date
    _sql="select distinct user_name,gw_id,'%s' as day,'1' as flag from tb_user_login_info" % date
    rs = sql_context.sql(_sql)
    re_rdd = rs.map(lambda r:(r.mac+"_"+r.gw_id,r.day+sep+r.flag))\
        .reduceByKey(lambda vs:vs[0],1)

    list =[]
    for t in re_rdd.collect():
        line = t[0]+sep+t[1]
        list.append(line)

    BaseService()._write_file(list, output)

    sc.stop()