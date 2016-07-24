# -*- coding: UTF-8 -*-

from hanzi2pinyin import hanzi2pinyin
from bblink.graphite.dao import MysqlDao

__author__ = 'kevin'

# kafka-graphite
def load_gwid_hosid2():
    dic={}
    _sql = "SELECT t.`gw_id`,t.`hospital_id`,t2.`hospital_name` FROM bblink_wifi_info t ,`bblink_hospital_info` t2 WHERE t.`hospital_id` IS NOT NULL AND t2.`hospital_id` = t.`hospital_id` AND t2.`hospital_name` IS NOT NULL ORDER BY t.`hospital_id`"
    rs = MysqlDao().findWithQuery(_sql)
    for r in rs:
        gwid = r[0]
        hos_name = r[2]
        dic[gwid]=hanzi2pinyin(hos_name)
        #print r
        #break

    return dic

def tran_parent(str):
    import re
    py = hanzi2pinyin(str)
    py = re.sub(r'[^a-zA-Z0-9 ]',r'-',py)
    '''
    if py.__contains__('\uff08'):
        py = py.replace('\uff08','[',100).replace('\uff09',']',100)
        py = py.replace('（','[',100).replace('）',']',100)
    '''

    return py

# kafka-graphite
def load_gwid_hosid():
    #dic={}
    _sql = "SELECT t.`gw_id`,t.`hospital_id`,t2.`hospital_name` FROM bblink_wifi_info t ,`bblink_hospital_info` t2 WHERE t.`hospital_id` IS NOT NULL AND t2.`hospital_id` = t.`hospital_id` AND t2.`hospital_name` IS NOT NULL ORDER BY t.`hospital_id`"
    rs = MysqlDao().findWithQuery(_sql)
    for r in rs:
        gwid = r[0]
        hos_name = tran_parent(r[2])
        #dic[gwid] = hos_name
        print(hos_name)

if __name__ == '__main__':
    load_gwid_hosid()
    '''
    dic = load_gwid_hosid2()
    for key in dic.keys():
        print dic[key]
    '''
