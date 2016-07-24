import sys, os

from pyspark import SparkConf, SparkContext, SQLContext
from bblink.back.util import DateUtil

# mysql to parquet file :log_login_flow
if __name__ == '__main__':

    if len(sys.argv) != 3:
        print("Usage: spark_mysql2parquet.py <date=20151010> <date=20151011>")
        sys.exit(-1)

    today,tomorrow = sys.argv[1:]
    begin = DateUtil.datestr_to_mill(today)
    end = DateUtil.datestr_to_mill(tomorrow)

    _output = 'hdfs://master:8020/impala/parquet/back/back-portal-wechatflowlog/dat=%s' % today
    _master = 'spark://master:7077'
    '''
    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home
    os.environ['SPARK_CLASSPATH'] = '/opt/cloud/spark/lib/mysql-connector-java-5.1.34.jar'
    '''
    conf = (SparkConf()
            .setMaster(_master)
            .setAppName("mysql2parquet_app")
            .set("spark.kryoserializer.buffer.mb", "256"))
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # _url = 'jdbc:mysql://192.168.0.173:3306/bblink_data?user=bblink_hos&password=bblink_hos'
    _url = 'jdbc:mysql://10.51.33.120:3306/bblink_hoswifi?user=bblink_hoswifi&password=KzrEuXEb92jW20g6qA2'

    sql = "(select concat('portal') as dfrom,gw_id as gwid,convert(hos_id,char) as hosid,convert(supp_id,char) as suppid,user_mac as usermac," \
          "type,log_time as logtime,auth_type as authtype from log_wechat_flow" \
          " where log_time >= %s and log_time <%s ) as t"
    _sql = sql % (begin, end)

    df = sqlContext.read.format('jdbc').options(url=_url, dbtable=_sql).load()

    df.write.parquet(_output)

    sc.stop()
