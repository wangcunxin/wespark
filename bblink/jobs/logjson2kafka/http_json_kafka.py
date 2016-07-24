# -*- coding: utf-8 -*-

from __future__ import print_function

import os, sys, socket, time, threading,random,json,datetime

from kafka import SimpleProducer, KafkaClient, KeyedProducer
from restclient import GET, POST, PUT, DELETE
from bblink.jobs.logjson2kafka.logger import *

__author__ = 'kevin'


def getMsg(url):
    headers = {
        'Accept': 'application/vnd.bblink.remote-access.v1+json',
        'Content-Type': 'application/json; charset=UTF-8'
    }
    msg = GET(url, headers)
    msgObj = json.loads(msg)

    return msgObj

def getList(ele,supplier,url_gw):
    create_time = str(int(time.time()))
    gwid = ele['gwid']
    wan_ip = ele['wan_ip']
    status = str(ele['status'])
    ap_count = str(ele['ap_count'])
    ap_online_count = str(ele['ap_online_count'])
    ap_offline_count = str(ele['ap_offline_count'])
    online_user_count = str(ele['online_user_count'])
    feel_user_count = str(ele['feel_user_count'])
    total_up = str(ele['total_up'])
    total_down=str(ele['total_down'])
    up_speed=str(ele['up_speed'])
    down_speed=str(ele['down_speed'])
    cpu_used = str(ele['cpu_used'])
    memory_used = str(ele['memory_used'])
    starttime_long = str(ele['starttime_long'])
    heatbreak_time = str(ele['heatbreak_time'])
    total_wideband = str(ele['total_wideband'])
    wideband_perused = str(ele['wideband_perused'])

    url = url_gw % gwid
    msg_gw = getMsg(url)
    if msg_gw['errmsg'] == 'success':
        online_user_count = str(msg_gw['total'])

    '''
    supplier,设备ID,hosID,WAN口IP，工作状态，
    AP总数，AP在线数，AP离线数，在线用户数，感知用户数，
    CPU占用，内存占用，在线时长，心跳时间，上行流，
    下行流，上行速率，下行速率，总带宽大小，带宽使用比例,
    记录时间
    '''
    cols = [supplier, gwid, '', wan_ip, status,
            ap_count, ap_online_count, ap_offline_count, online_user_count, feel_user_count,
            cpu_used, memory_used, starttime_long, heatbreak_time,total_up,
           total_down,up_speed, down_speed,total_wideband, wideband_perused,
            create_time]
    return cols
# huanchuang filed is different
def getList_Huan(ele,supplier,url_gw):
    create_time = str(int(time.time()))
    gwid = ele['gwid']
    wan_ip = ele['wanIp']
    status = str(ele['status'])
    ap_count = str(ele['apCount'])
    ap_online_count = str(ele['apOnlineCount'])
    ap_offline_count = str(ele['apOfflineCount'])
    online_user_count = str(ele['onlineUserCount'])
    feel_user_count = str(ele['feelUserCount'])
    total_up = str(ele['totalUp'])
    total_down=str(ele['totalDown'])
    up_speed=str(ele['upSpeed'])
    down_speed=str(ele['downSpeed'])
    cpu_used = str(ele['cpuUsed'])
    memory_used = str(ele['memoryUsed'])
    starttime_long = str(ele['starttimeLong'])
    heatbreak_time = str(ele['heatbreakTime'])
    total_wideband = str(ele['totalWideband'])
    wideband_perused = str(ele['widebandPerused'])

    '''
    supplier,设备ID,hosID,WAN口IP，工作状态，
    AP总数，AP在线数，AP离线数，在线用户数，感知用户数，
    CPU占用，内存占用，在线时长，心跳时间，上行流，
    下行流，上行速率，下行速率，总带宽大小，带宽使用比例,
    记录时间
    '''
    cols = [supplier, gwid, '', wan_ip, status,
            ap_count, ap_online_count, ap_offline_count, online_user_count, feel_user_count,
            cpu_used, memory_used, starttime_long, heatbreak_time,total_up,
           total_down,up_speed, down_speed,total_wideband, wideband_perused,
            create_time]
    return cols

def getLines(supplier, url_router, url_gw):
    lines = []
    _sep = "\t"
    msg_router = getMsg(url_router)

    if msg_router['errmsg'] == 'success':
        gw_qty = str(msg_router['total'])
        logger.info(supplier + "-->gw_qty:" + gw_qty)
        data = msg_router['data']
        for ele in data:
            cols = getList_Huan(ele,supplier,url_gw)
            line = _sep.join(cols).encode("utf-8")
            lines.append(line)

            if lines.__len__() == 2:
                logger.debug(lines)
                break
    return lines

def sendMsg(topic, lines):
    if lines.__len__() > 0:
        brokers = 'cdh-slave0:9092,cdh-slave1:9092,cdh-slave2:9092'
        kafka = KafkaClient(brokers)
        producer = KeyedProducer(kafka)
        for line in lines:
            ran = "_" + str(random.randint(0, 10))
            producer.send_messages(topic, topic + ran, line)
        producer.stop()


def timer_start():
    t = threading.Timer(60, event_func)
    t.start()


# get http log json,transfer,send to kafka
def event_func():
    begin = datetime.datetime.now();
    url_router_ikuai = 'http://112.65.205.80:12048/partner/2014071717480672279/routers'
    url_router_abloomy = 'http://112.65.205.107:80/rest/partner/bblink/routers'
    url_router_gbcom = 'http://112.65.205.70:8888/ccsv3/rest/partner/bblink/routers'
    # omit
    url_gw_ikuai = "http://112.65.205.80:12048/router/%s/auth/users?limit=100&skip=20&partner=2014071717480672279"
    url_gw_abloomy = "http://112.65.205.107/rest/router/%s/auth/users"
    url_gw_gbcom = ""

    try:
        #lines = getLines("ikuai", url_router_ikuai, url_gw_ikuai)
        topic = 'wifi-acdeviceinfo'
        #sendMsg(topic, lines)

        #lines = getLines("abloomy", url_router_abloomy, url_gw_abloomy)
        #sendMsg(topic, lines)

        lines = getLines("gbcom", url_router_gbcom, url_gw_gbcom)
        sendMsg(topic, lines)

    except Exception, e:
        logger.error(e)

    end = datetime.datetime.now()
    logger.info("cost:" + str(end - begin))

    timer_start()


if __name__ == '__main__':
    timer_start()

    while True:
        time.sleep(60)
