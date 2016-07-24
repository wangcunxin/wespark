import sys,os

from pyspark import SparkConf, SparkContext, SQLContext

def printx(x):
    print(x)

# mysql to parquet file
if __name__ == '__main__':

    if len(sys.argv) != 3:
        print("Usage: spark_mysql2parquet.py <sql> <outDir>")
        sys.exit(-1)

    sql,outDir = sys.argv[1:]
    # /impala/parquet/back/back-portal-loginflowlog
    _output = 'hdfs://master:8020/impala/parquet/back/'+outDir
    _master = 'spark://master:7077'
    '''
    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home
    os.environ['SPARK_CLASSPATH'] = '/home/lidl/cloud/mysql-connector-java-5.1.36.jar'
    '''
    conf = (SparkConf()
            .setMaster(_master)
            .setAppName("mysql2parquet_app")
            .set("spark.kryoserializer.buffer.mb", "256"))
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    # 120.26.140.153 -ubblink_hoswifi -pmh9lVOY6xfpz1Rh
    #_url = 'jdbc:mysql://192.168.0.173:3306/bblink_data?user=bblink_hos&password=bblink_hos'
    _url = 'jdbc:mysql://10.51.33.120:3306/bblink_hoswifi?user=bblink_hoswifi&password=KzrEuXEb92jW20g6qA2'

    '''
    log_wechat_flow log_login_flow
    select concat('portal') as dfrom,log_type,hos_id,supp_id,gw_id,user_mac,login_version,forward_version,arrive_version,login_type,is_new_user,log_time from log_login_flow
    select concat('portal') as dfrom,gw_id,hos_id,supp_id,user_mac,type,log_time,auth_type from log_wechat_flow
    '''

    _sql = '( %s )as t' % sql

    df = sqlContext.read.format('jdbc').options(url=_url, dbtable=_sql).load()

    df.saveAsParquetFile(_output)

    #df.rdd.foreach(printx)
    sc.stop()