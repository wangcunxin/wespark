# -*- coding: utf-8 -*-
import datetime
import time,sys

from pyspark import SparkContext, SQLContext, SparkConf

from bblink.website.sitepvv3.log_handler import LogHandler
from bblink.website.sitepvv3.configure import ConfigSpark, ConfigSparkPath
from bblink.website.sitepvv3.logger import *

__author__ = 'kevin'


def dateConvLongtime(date):
    if date != None and date != '' and date != 'None':
        s = time.mktime(time.strptime(date, "%Y-%m-%d %H:%M:%S"))
        return s
    else:
        return getTime_format()


def getTime_format():
    return datetime.datetime.strptime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")


def getKey(item):
    # print(type(item))
    return item[0]


def tolist(x):
    destlist = []
    for item in iter(x):
        tmparray = item.split('\t')
        # createtime,pid,prepid,pretime,originalLine
        destlist.append([dateConvLongtime(tmparray[10]), tmparray[0], tmparray[6], tmparray[7], item])

    writelines = []
    sortedList = []
    try:
        sortedList = sorted(destlist, key=getKey, reverse=False)
    except Exception, e:
        pass

    if len(sortedList) == 1:
        writelines.append((None, sortedList[0][4] + '\t0\t1\t1\t1'))
    elif len(sortedList) > 1:
        listlength = len(sortedList)
        countIndex = 0
        for outeritem in iter(sortedList):
            currentAccessTimelong = 0
            singleAccess = 0
            firstAccess = 0
            lastAccess = 0
            originLine = outeritem[4]
            # judge if firstaccess
            if countIndex == 0:
                firstAccess = 1

            # fill page current access time long
            for inneritem in iter(sortedList):
                if outeritem[1] == inneritem[2]:
                    currentAccessTimelong = inneritem[3]
                    break

            countIndex += 1
            # judge if lastaccess after +=1
            if countIndex == listlength:
                lastAccess = 1
            '''
            __sep = '\t'
            tmp_arr = [originLine.decode('utf-8'),str(currentAccessTimelong),str(singleAccess),str(firstAccess),str(lastAccess)]
            new_line = __sep.join(tmp_arr)
            writelines.append((None, new_line))
            '''

            try:
                writelines.append((None, originLine.decode('utf-8') + "\t" + str(currentAccessTimelong) + "\t" + str(
                    singleAccess) + "\t" + str(firstAccess) + "\t" + str(lastAccess)))
            except:
                writelines.append((None, originLine + "\t" + str(currentAccessTimelong) + "\t" + str(
                    singleAccess) + "\t" + str(firstAccess) + "\t" + str(lastAccess)))

    return writelines


if __name__ == '__main__':
    DAY_OFFSET = 1
    # --set datetime
    now = datetime.datetime.now();
    pro_time = now - datetime.timedelta(days=DAY_OFFSET)
    dest_time_str = pro_time.strftime("%Y%m%d")
    dest_time_str = sys.argv[1]
    master = ConfigSpark.master

    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    input = "/input/loghandle/20151027-*"
    output = "/output/loghandle/" + dest_time_str
    '''
    input = ConfigSparkPath.input_sitepvv3 % dest_time_str
    output = ConfigSparkPath.output_sitepvv3 % dest_time_str
    '''
    __separator = ConfigSpark.separator
    app_name = 'log_handle_app'

    conf = (SparkConf()
            .setMaster(master)
            .setAppName(app_name)
            .set("spark.kryoserializer.buffer.mb", "256"))

    sc = SparkContext(pyFiles=['log_handler.py', 'url_handler.py','logger.py'],conf=conf)
    sql_context = SQLContext(sc)

    lines = sc.textFile(input)
    parts = lines.map(lambda l: l.split(__separator)).filter(lambda x: len(x) == 17)

    lines_new = parts.map(lambda line: LogHandler().handle(line))

    logData = lines_new.filter(lambda line: len(line.split(__separator)) == 28).map(
        lambda line: (line.split(__separator)[1], line))

    reduceData = logData.groupByKey().mapValues(tolist).values().flatMap(list)

    reduceData.saveAsHadoopFile(output,
                                'org.apache.hadoop.mapred.TextOutputFormat',
                                'org.apache.hadoop.io.NullWritable',
                                'org.apache.hadoop.io.Text')
    logger.info("completed")

    sc.stop()
