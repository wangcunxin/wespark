from __future__ import print_function
import os
import sys
import datetime
import time

from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from bblink.website.sitepvv3.log_handler import LogHandler
from bblink.website.sitepvv3 import logger
from bblink.website.sitepvv3.configure import  ConfigAddr, ConfigSpark

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
            __sep = '\t'
            new_line=u''

            try:
                tmp_arr = [originLine.decode('utf-8'),str(currentAccessTimelong),str(singleAccess),str(firstAccess),str(lastAccess)]
                new_line = __sep.join(tmp_arr)
            except:
                tmp_arr = [originLine,str(currentAccessTimelong),str(singleAccess),str(firstAccess),str(lastAccess)]
                new_line = __sep.join(tmp_arr)

            writelines.append((None, new_line))

    return writelines

if __name__ == "__main__":

    __separator = ConfigSpark.separator

    if len(sys.argv) != 3:
        print("Usage: spark_streaming.py <master> <batchInterval>", file=sys.stderr)
        exit(-1)

    master, batchInterval = sys.argv[1:]
    appName = 'kafka_topic_streaming_app'

    #logger.info("--->" + master + " " + batchInterval)

    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    sc = SparkContext(master, appName)
    ssc = StreamingContext(sc, int(batchInterval))

    topics ="site-sitePVv1".split(',')
    kafkaParams = {"metadata.broker.list": ConfigAddr.brokers}
    kafka_stream = KafkaUtils.createDirectStream(ssc, topics, kafkaParams)
    msgs = kafka_stream.map(lambda x: x[1])
    lines = msgs.map(lambda msg: msg.split(__separator)).filter(lambda x:len(x)==17)

    lines_new = lines.map(lambda line: LogHandler().handle(line))

    lines_new_mid = lines_new.filter(lambda line: len(line.split(__separator)) == 28)\
        .map(lambda line: (line.split(__separator)[1], line))

    lines_new_term = lines_new_mid.groupByKey().mapValues(tolist).values().flatMap(list)
    lines_new_term.pprint()

    def process(time, rdd):
        logger.debug("========= %s =========" % str(time))
        try:
            if rdd.count() == 0:
                return
            _time_str=time.strftime("%Y%m%d")
            rdd.saveAsHadoopFile(
                "hdfs://hadoop:9000/output/site/"+_time_str,
                'org.apache.hadoop.mapred.TextOutputFormat',
                'org.apache.hadoop.io.NullWritable',
                'org.apache.hadoop.io.Text')

        except Exception, e:
            logger.error("-->"+e.__str__())


    lines_new_term.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()
