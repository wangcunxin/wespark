# -*- coding: utf-8 -*-
from __future__ import print_function

import os, sys, socket, time, threading, random,re

from hanzi2pinyin import hanzi2pinyin
from pyspark.sql.types import StructField, StringType, StructType
from pyspark import SparkContext, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from bblink.graphite.dao import MysqlDao
from bblink.graphite.logger import *

__author__ = 'kevin'


# send msg to graphite
def sendmsg(msgs=None):
    if (msgs == None):
        return
    CARBON_SERVER = '112.65.205.79'
    CARBON_PORT = 2003
    sock = socket.socket()
    sock.connect((CARBON_SERVER, CARBON_PORT))
    for msg in msgs:
        sock.sendall(msg)
    sock.close()


class SparkUtil:
    def getSqlContextInstance(self, sparkContext):
        if ('sqlContextSingletonInstance' not in globals()):
            globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
        return globals()['sqlContextSingletonInstance']


class BaseService(object):
    # parquet
    def _write_parquet(self, df, output_path):

        try:
            # df.show()
            logger.debug(output_path)
            df.coalesce(1).write.mode('append').parquet(output_path)
        except Exception, e:
            logger.error(e.__str__())

    # file:file:///
    def _write_file(self, list, output_path):
        fo = None
        try:
            fo = open(output_path, 'w')
            for line in list:
                fo.write(line + '\n')

        except Exception, e:
            print(e.__str__())
        finally:
            try:
                if fo != None:
                    fo.close()
            except Exception, e:
                print(e.__str__())


class GraphiteService(BaseService):
    # compute send msg to graphite
    def compute_send(self, sql_context):
        _sql = "select supplier,acid,hosid,userOnlineQty,upflow,downflow,upspeed,downspeed from wifi_acdeviceinfo"
        rs = sql_context.sql(_sql)
        for r in rs.collect():
            logger.debug(r[0])


# kafka-graphite
def load_gwid_hosid(dic):
    _sql = "SELECT t.`gw_id`,t.`hospital_id`,t2.`hospital_name` FROM bblink_wifi_info t ,`bblink_hospital_info` t2 WHERE t.`hospital_id` IS NOT NULL AND t2.`hospital_id` = t.`hospital_id` AND t2.`hospital_name` IS NOT NULL ORDER BY t.`hospital_id`"
    rs = MysqlDao().findWithQuery(_sql)
    for r in rs:
        gwid = r[0]
        hos_name = hanzi2pinyin(r[2])
        hos_name = re.sub(r'[^a-zA-Z0-9 ]',r'-',hos_name)
        dic[gwid] = hos_name
    # print(dic)
    sched_load_gwid_hosid(dic)


def sched_load_gwid_hosid(dic):
    t = threading.Timer(60 * 60 * 24, load_gwid_hosid, (dic,))
    t.start()


if __name__ == '__main__':

    __separator = '\t'

    if len(sys.argv) != 3:
        print("Usage: spark_streaming.py <master> <batchInterval>", file=sys.stderr)
        exit(-1)

    master, batchInterval = sys.argv[1:]
    appName = 'kafka_topic_streaming_app'
    logger.debug("--->" + master + " " + batchInterval)

    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    # get gwid and hosid
    dic = {}
    load_gwid_hosid(dic)
    sched_load_gwid_hosid(dic)

    sc = SparkContext(master, appName)
    ssc = StreamingContext(sc, int(batchInterval))

    topics = ["wifi-acdeviceinfo", "site-sitePVv3"]
    brokers = 'cdh-slave0:9092,cdh-slave1:9092,cdh-slave2:9092'
    kafkaParams = {"metadata.broker.list": brokers}

    kafka_stream = KafkaUtils.createDirectStream(ssc, topics, kafkaParams)

    topic_acdeviceinfo = kafka_stream.filter(lambda x: x[0].__contains__("wifi-acdeviceinfo")) \
        .map(lambda x: x[1]).map(lambda x: x.split(__separator)).filter(lambda x: len(x) == 21)

    topic_sitePVv3 = kafka_stream.filter(lambda x: x[0].__contains__("site-sitePVv3")).map(lambda x: x[1]) \
        .map(lambda msg: msg.split(__separator)).filter(lambda x: len(x) == 17)


    def process_acdeviceinfo(_time, rdd):
        logger.debug("========= %s =========" % str(_time))

        try:
            if rdd.count() == 0:
                return

            logger.info(rdd.count())
            '''
            supplier,设备ID,hosID(*),WAN口IP，工作状态，
            AP总数，AP在线数，AP离线数，在线用户数，感知用户数，
            CPU占用，内存占用，在线时长，心跳时间，上行流，
            下行流，上行速率，下行速率，总带宽大小，带宽使用比例,
            记录时间
            '''
            int_time = int(time.time())
            message = 'wifi.stat.%s.%s %s %d\n'

            for line in rdd.collect():
                supplier = line[0]
                acid = line[1]
                int_time=int(line[20])
                # acid match hosid
                hos_name = dic.get(acid, None)
                msg_list = []
                if hos_name == None:
                    hos_name = acid

                userOnlineQty = line[8]
                upflow = line[14]
                downflow = line[15]
                upspeed = line[16]
                downspeed = line[17]
                msg = message % (hos_name, "users", userOnlineQty, int_time)
                msg_list.append(msg)
                msg = message % (hos_name, "upflow", upflow, int_time)
                msg_list.append(msg)
                msg = message % (hos_name, "downflow", downflow, int_time)
                msg_list.append(msg)
                msg = message % (hos_name, "upspeed", upspeed, int_time)
                msg_list.append(msg)
                msg = message % (hos_name, "downspeed", downspeed, int_time)
                msg_list.append(msg)

                # print(msg_list)
                sendmsg(msg_list)

            '''
            sql_context = SparkUtil().getSqlContextInstance(rdd.context)
            schema_string = "supplier,acid,hosid,wanip,status," \
                            "apTotal,apOnlineQty,apOfflineQty,userOnlineQty,userDetectedQty," \
                            "cpu,ram,onlineTime,heartbeat,upflow," \
                            "downflow,upspeed,downspeed,totalBandwidth,usedBandwidth," \
                            "recordTime"
            fields = [StructField(field_name, StringType(), True) for field_name in schema_string.split(",")]
            schema = StructType(fields)

            schema_rdd = sql_context.createDataFrame(rdd, schema)
            schema_rdd.registerTempTable("wifi_acdeviceinfo")

            GraphiteService().compute_send(sql_context)
            '''
        except Exception, e:
            logger.error(e)


    def process_sitePVv3(_time, rdd):
        logger.debug("========= %s =========" % str(_time))

        try:
            if rdd.count() == 0:
                return
            # pv
            pv = rdd.count()
            int_time = int(time.time())
            message = 'wifi.stat.pv %d %d\n' % (pv, int_time)
            list = [message]
            sendmsg(list)


        except Exception, e:
            logger.error(e)


    topic_acdeviceinfo.foreachRDD(process_acdeviceinfo)
    topic_sitePVv3.foreachRDD(process_sitePVv3)

    ssc.start()
    ssc.awaitTermination()
