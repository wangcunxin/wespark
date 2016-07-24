# -*- coding: utf-8 -*-

import datetime
import logging
import random
import time

import requests
import thread
from kafka import KafkaClient, KeyedProducer

__author__ = 'kevin'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='/var/log/wifi/http_json_kafka_gbcom.log',
                    filemode='a')
logger= logging.getLogger('http_json_kafka')

def getMsg(url):
    '''
    headers = {
        'Accept': 'application/vnd.bblink.remote-access.v1+json',
        'Content-Type': 'application/json; charset=UTF-8'
    }
    '''
    # msg = requests.get(url, headers)
    msg = requests.get(url).json()
    return msg

def getList(ele,supplier,url_gw,create_time):
    create_time = str(int(create_time))
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
    online_user_count=  str(ele['online_user_count'])

    '''
    url = url_gw % gwid
    msg_gw = getMsg(url)
    if msg_gw['errmsg'] == 'success':
        online_user_count = str(msg_gw['total'])
    '''

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
    logger.info(cols)
    return cols
# huanchuang filed is different
def getList_Huan(ele,supplier,url_gw,create_time):
    create_time = str(int(create_time))
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
    logger.info(cols)
    return cols

def getLines(supplier, url_router, url_gw):
    lines = []
    _sep = "\t"
    create_time=time.time()
    logger.info("create_time is %s " % create_time)
    msg_router = getMsg(url_router)

    if msg_router['errmsg'] == 'success':
        gw_qty = str(msg_router['total'])
        logger.info(supplier + "-->gw_qty:" + gw_qty)
        data = msg_router['data']
        for ele in data:
	    if supplier=='gbcom':
                cols = getList_Huan(ele,supplier,url_gw,create_time)
                line = _sep.join(cols).encode("utf-8")
                lines.append(line)
            else:
                cols = getList(ele,supplier,url_gw,create_time)
                line = _sep.join(cols).encode("utf-8")
                lines.append(line)

    return lines

def sendMsg(topic, lines):
    if lines.__len__() > 0:
        brokers = '10.117.181.44:9092,10.117.108.143:9092,10.117.21.79:9092'
        kafka = KafkaClient(brokers)
        producer = KeyedProducer(kafka)
        for line in lines:
            ran = "_" + str(random.randint(0, 10))
            producer.send_messages(topic, topic + ran, line)
        producer.stop()


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

        paras={
            "gbcom":('gbcom',url_router_gbcom, url_gw_gbcom),
            "abloomy":('abloomy',url_router_abloomy, url_gw_abloomy),
            "ikuai":('ikuai',url_router_ikuai, url_gw_ikuai)
        }

        _p=paras.get('gbcom')
        lines = getLines(_p[0],_p[1],_p[2])
        sendMsg(topic, lines)

    except Exception, e:
        logger.error(e)

    end = datetime.datetime.now()
    logger.info("cost:" + str(end - begin))

if __name__ == '__main__':
    running=False
    while True:
        _date=datetime.datetime.now()
        _m= _date.minute
        if _m % 5==0:
            if running==False:
                logger.info("scheduler fired")
                thread.start_new_thread(event_func,())
                running=True
        else:
            running=False
        time.sleep(1)
