import datetime, sys

from pyspark.sql.types import StringType
from pyspark import SparkContext, SQLContext

from bblink.website.pageflow.logger import *
from bblink.website.pageflow.dao import MysqlDao

__author__ = 'kevin'


def mill_date_str(timeStamp):
    mill = long(timeStamp) / 1000
    dateArray = datetime.datetime.utcfromtimestamp(mill)
    ret = dateArray.strftime("%Y-%m-%d")
    return ret


def bytearray_str(byte_array):
    return str(byte_array)

# drop
if __name__ == '__main__':
    # --set datetime
    DAY_OFFSET = 1
    now = datetime.datetime.now()
    pro_time = now - datetime.timedelta(days=DAY_OFFSET)
    day = pro_time.strftime("%Y%m%d")

    master = "spark://hadoop:7077"
    appName = "spark_pageflow_outflow"
    input = "/impala/parquet/site/site-pageflowv1/dat=%s" % day

    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    sc = SparkContext(master, appName)
    sql_context = SQLContext(sc)
    sql_context.registerFunction("to_day", lambda x: mill_date_str(x), StringType())
    sql_context.registerFunction("to_str", lambda x: bytearray_str(x), StringType())

    parquet_df = sql_context.read.parquet(input)
    sql_context.registerDataFrameAsTable(parquet_df, "site_pageflowv1")

    _sql = "select to_str(url),to_day(createtime) day,count(1) pv,count(distinct to_str(guuid)) uv " \
           "from site_pageflowv1 where dat= %s and to_str(name)='outflow' " \
           "group by to_str(url),to_day(createtime)" % day

    rs_df = sql_context.sql(_sql)
    rs = rs_df.collect()
    logger.info("---->" + str(len(rs)))

    list = []
    for r in rs:
        url = r[0]
        day = r[1]
        pv = r[2]
        uv = r[3]
        t = (day, url, pv, uv)
        list.append(t)
        logger.debug(t)

    _sql = "insert into link_url_pv_uv(count_date,short_link_url,url_pv,url_uv) values(%s,%s,%s,%s)"
    MysqlDao().insertMany(_sql, list)

    logger.info("---->finished")

    sc.stop()
