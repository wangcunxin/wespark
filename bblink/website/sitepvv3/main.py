from __future__ import print_function
import datetime

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

from bblink.website.sitepvv3.sitepvv3_service import Sitepvv3Service
from bblink.website.sitepvv3.configure import ConfigSpark, ConfigSparkPath

__author__ = 'kevin'

class Fun():

    def __init__(self):
        pass

    def datediff(self,x,k):
        dateStart = self.dateConvLongtime(x)
        dateEnd = self.dateConvLongtime(k)

        if dateStart!=None and dateEnd!=None:
            return (dateStart-dateEnd).days
        else:
            return 0


    def dateConvLongtime(self,date):
        if date!=None and date!='' and date!='None':
            s = datetime.datetime.strptime(date,"%Y-%m-%d %H:%M:%S")
            return s
        else:
            return self.__getTime_format()

    def to_date(self,dateStr):
        if dateStr!=None and dateStr!='' and dateStr!='None':
            return datetime.datetime.strptime(dateStr,"%Y-%m-%d %H:%M:%S").date()
        else:
            return self.__getTime_format().date()

    def to_hour(self,dateStr):
        if dateStr!=None and dateStr!='' and dateStr!='None':
            return datetime.datetime.strptime(dateStr,"%Y-%m-%d %H:%M:%S").hour
        else:
            return self.__getTime_format().hour

    def __getTime_format(self):
        return datetime.datetime.strptime("1907-01-01 00:00:00","%Y-%m-%d %H:%M:%S")
'''
compute website data, replace bin_v4=spark_v4,but there are too many tasks to refactor,so pause
'''
if __name__ == '__main__':

    DAY_OFFSET=1
    #--set datetime
    now =datetime.datetime.now()
    pro_time=now-datetime.timedelta(days=DAY_OFFSET)
    dest_time_str=pro_time.strftime("%Y%m%d")

    master = ConfigSpark.master
    input = ConfigSparkPath.input_sitepvv3 % dest_time_str
    output = ConfigSparkPath.output_sitepvv3 % dest_time_str
    separator = ConfigSpark.separator
    app_name = 'sitepvv3_app'

    sc = SparkContext(master, app_name)
    sql_context = SQLContext(sc)

    lines = sc.textFile(input)
    parts = lines.map(lambda l: l.split(separator)).filter(lambda x: len(x) == 32)

    schema_string = "site_id,site_uuid,site_uuid_ctime,ptitle,url," \
                    "referrer,prevPID,attime,resolution,ip," \
                    "ctime,language,cookie_enabled,ua,uuid," \
                    "uuid_ctime,browser,os,tag_key,supp_id," \
                    "gw_id,portal_version,from_page,channel_id,channel_list_id," \
                    "content_id,advid,appid,spenttime,assingleaccess," \
                    "asfirstaccess,aslastaccess"
    fields = [StructField(field_name, StringType(), True) for field_name in schema_string.split(separator)]
    schema = StructType(fields)

    schema_rdd = sql_context.createDataFrame(parts, schema)
    schema_rdd.registerTempTable("sitepvv3")

    sql_context.registerFunction("to_date", lambda x:Fun().to_date(x), DateType())
    sql_context.registerFunction("datediff",lambda x,k:Fun().datediff(x,k),IntegerType())

    sql_context.registerFunction("hour",lambda x:Fun().to_hour(x),IntegerType())
    sql_context.registerFunction("str_conver_int",lambda x:int(x),IntegerType())

    begin_time = dest_time_str
    end_time = dest_time_str
    Sitepvv3Service().exec_file(sql_context,begin_time,end_time)

    sc.stop()
