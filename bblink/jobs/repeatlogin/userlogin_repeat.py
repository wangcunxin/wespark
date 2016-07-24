from __future__ import print_function

import sys

from pyspark import SQLContext, SparkContext
from pyspark.sql.types import *

from bblink.jobs.repeatlogin.dao import MysqlDao
from bblink.jobs.repeatlogin.logger import *
from bblink.jobs.repeatlogin.userlogin_repeat_service import UserLoginRepeatService
from bblink.jobs.util import DateUtil

__author__ = 'kevin'

# sql:spark_sql mysql_delete_sql mysql_insert_sql
class ConfigPortalSql:
    def __init__(self):
        pass

    # == mysql ==

    # --select mysql--
    select_mysql_hos_gw_sup = "SELECT t.`hospital_id`,t.`gw_id`,t.`supplier_id` FROM bblink_wifi_info t WHERE t.`hospital_id` IS NOT NULL ORDER BY t.`hospital_id`"
    select_mysql_portal_url_pv = "select pv from tb_portal_url_pv where ymd='%s'"

    # --delete mysql--

    # --insert mysql--

    # --update mysql--
    update_portal_url_pv = "update tb_portal_url_pv set pv = %s where ymd = '%s'"


    # == spark ==

    # --select spark--
    # portal_url
    select_portal_url = "select * from portal_url"

    # userloginrepeat
    select_userlogin_repeat = "select get_date(login_time) as day,hos_id,mac,login_time from wxcity_userlogin_info where login_time >= '%s 00:00:00' and login_time <='%s 23:59:59' order by day,hos_id,mac,login_time"
    select_userlogin_repeat_sta = "select day,hos_id,sum(t2),sum(t5),sum(t10),sum(t30),sum(t60) from repeat_login_list group by day,hos_id"


if __name__ == '__main__':

    if len(sys.argv) != 5:
        print("Usage: spark_streaming.py <master> <begin> <end> <input>", file=sys.stderr)
        exit(-1)

    master, time_begin, time_end, input = sys.argv[1:]
    input_path = input + '/' + time_begin + '.csv'
    logger.info("--->" + master + " " + input_path)

    sc = SparkContext(master, 'wxcity_userlogin_repeat_app')
    sql_context = SQLContext(sc)

    lines = sc.hadoopFile(input,
                          'org.apache.hadoop.mapred.TextInputFormat',
                          'org.apache.hadoop.io.LongWritable',
                          'org.apache.hadoop.io.Text'
                          )

    rs_tuples = MysqlDao().findWithQuery(ConfigPortalSql.select_mysql_hos_gw_sup)
    gwid_hosid_dict = {}
    for r in rs_tuples:
        hos_id = str(r[0])
        gw_id = r[1]
        gwid_hosid_dict[gw_id] = hos_id
    logger.debug('-->gwid_hosid:' + str(gwid_hosid_dict.__len__()))
    users = lines.map(lambda x: x[1].split(',')).filter(lambda x: len(x) == 17) \
        .map(lambda p: (p[0].strip(), p[1].strip(), p[2].strip(), p[3].strip(), p[4].strip(), \
                        p[5].strip(), p[6].strip(), p[7].strip(), p[8].strip(), p[9].strip(), \
                        p[10].strip(), p[11].strip(), p[12].strip(), p[13].strip(), p[14].strip(), \
                        p[15].strip(), p[16].strip(), gwid_hosid_dict.get(p[1].strip(), "")))
    logger.debug('-->users:' + str(users.count()))
    schema_string = "id gw_id supp_id user_id user_type " \
                    "user_name login_time logout_time mac ip " \
                    "user_agent download_flow upload_flow os browser " \
                    "ratio batch_no hos_id"

    fields = [StructField(field_name, StringType(), True) for field_name in schema_string.split(' ')]
    schema = StructType(fields)
    schema_users = sql_context.applySchema(users, schema)
    schema_users.registerTempTable("wxcity_userlogin_info")

    # regist udf
    sql_context.registerFunction("get_date", lambda x: DateUtil.str_to_date(x).date(), DateType())
    sql_context.registerFunction("date_diff", lambda x, k: DateUtil.date_diff(x, k), IntegerType())
    sql_context.registerFunction("get_hour", lambda x: DateUtil.str_to_date(x).hour(), IntegerType())
    sql_context.registerFunction("to_int", lambda x: int(x), IntegerType())
    sql_context.registerFunction("timestamp_diff", lambda x, k: DateUtil.timestamp_diff(x, k), IntegerType())

    lines_list = UserLoginRepeatService().exec_file(sql_context, time_begin, time_end)

    # group by day,hosid,(mac),2, 5, 10, 30, 60
    #repeat_list = sc.textFile(ConfigSparkPath.userlogin_repeat_path % time_begin).map(lambda line:line.split('\t')).filter(lambda x:len(x)==8)
    repeat_list = sc.parallelize(lines_list).map(lambda line:line.split('\t'))
    schema_string = "day hos_id mac t2 t5 " \
                    "t10 t30 t60"
    fields = [StructField(field_name, StringType(), True) for field_name in schema_string.split(' ')]
    schema = StructType(fields)
    schema_repeat_list = sql_context.applySchema(repeat_list, schema)
    schema_repeat_list.registerTempTable("repeat_login_list")
    UserLoginRepeatService().exec_file_other(sql_context,time_begin)

    sc.stop()

    logger.info("over")

'''
/input/wxcityuserinfo_day/2015-09-22.csv
31018584,b07e9b2f34918280edcbfa39d43b4f81,2,null,MOBILE,13598215460,2015-09-22 09:03:47,null,48:6b:2c:57:23:d0,101.226.61.186,Mozilla/5.0 (Linux; U; Android 4.3; zh-cn; vivo Y18L Build/JLS36C) AppleWebKit/537.36 (KHTML@ like Gecko)Version/4.0 MQQBrowser/6.1 Mobile Safari/537.36,0,0,Android 4.x,MOBILE_SAFARI4.0,1280*720,11111111
'''
