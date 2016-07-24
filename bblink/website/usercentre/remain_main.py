import datetime, time
import sys
from bblink.website.usercentre.logger import *
from bblink.website.usercentre.dao import MysqlDao

__author__ = 'kevin'

SEP = '_'


def days_add(dat, num):
    ret = None
    d1 = datetime.datetime.strptime(dat, "%Y%m%d")
    pro_time = d1 + datetime.timedelta(days=num)
    ret = pro_time.strftime("%Y%m%d")
    return ret


def days_minus(dat, num):
    ret = None
    d1 = datetime.datetime.strptime(dat, "%Y%m%d")
    pro_time = d1 - datetime.timedelta(days=num)
    ret = pro_time.strftime("%Y%m%d")
    return ret


def to_timestamp(dat):
    t0 = str(long(time.mktime(time.strptime(dat, '%Y%m%d'))) * 1000)
    return t0

def get_subscribe_list(sql):
    dao = MysqlDao()
    ret = []
    rs = dao.findWithQuery(sql)
    for r in rs:
        ret.append(r[0])
    return ret


if __name__ == '__main__':
    '''
    remain_rate=len(dat_1_list join dat_2_new_list)/len(dat_2_new_list)
        dat_2_new_list = dat_2_list diff (dat_2_list join dat_2_before_list)
    '''

    # --set datetime
    DAY_OFFSET = 1
    now = datetime.datetime.now()
    pro_time = now - datetime.timedelta(days=DAY_OFFSET)
    dat_1 = pro_time.strftime("%Y%m%d")
    logger.debug(dat_1)

    #dat_1 = '20160413'
    dat_2 = days_minus(dat_1, 1)
    app_id = 'wxac0f449b34ef6062'
    periods = [1, 2, 3, 4, 5, 6, 7, 10, 14, 21, 30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330, 360]
    sql_oneday = "SELECT DISTINCT open_id from user_subscribe WHERE app_id='%s' AND subscribe=1" \
          " AND create_time>=%s AND create_time<%s"
    sql_before = "SELECT DISTINCT open_id from user_subscribe WHERE app_id='%s' AND subscribe=1" \
          " AND create_time<%s;"

    # dat_1_list
    second_day = days_add(dat_1, 1)
    this_day_begin = to_timestamp(dat_1)
    this_day_end = to_timestamp(second_day)
    _sql = sql_oneday % (app_id, this_day_begin, this_day_end)
    dat_1_list = get_subscribe_list(_sql)


    remain_dic = {}
    for period in periods:
        this_day = days_minus(dat_1, period)
        second_day = days_add(this_day, 1)

        this_day_begin = to_timestamp(this_day)
        this_day_end = to_timestamp(second_day)

        _sql = sql_oneday % (app_id, this_day_begin, this_day_end)
        dat_n_list = get_subscribe_list(_sql)
        logger.debug(dat_n_list)

        if len(dat_n_list)==0:
            break

        _sql = sql_before % (app_id, this_day_begin)
        dat_n_before_list = get_subscribe_list(_sql)
        logger.debug(dat_n_before_list)

        old_list = set(dat_n_list).intersection(set(dat_n_before_list))
        new_list = set(dat_n_list).difference(set(old_list))
        remain_list = set(dat_1_list).intersection(set(new_list))

        remain_n_num = len(remain_list)
        new_n_num = len(new_list)
        logger.debug(remain_n_num,new_n_num)

        rate = 0
        if (remain_n_num > 0 and new_n_num > 0):
            r = float(remain_n_num) / float(new_n_num)
            logger.debug(r)
            rate = round(r, 4)

        remain_dic[str(period)+SEP+str(this_day)+SEP+str(new_n_num)] = rate

    logger.info(remain_dic)

    sql = "INSERT INTO user_subscribe_remain(app_id,subscribe_add,remain_rate,time_step,create_time)" \
          " VALUES(%s,%s,%s,%s,%s);"
    ret_list = []
    for k in remain_dic.keys():
        ks = k.split(SEP)
        period = ks[0]
        subscribe_add =ks[2]
        remain_rate = remain_dic[k]
        create_time = to_timestamp(ks[1])

        tup = (app_id, subscribe_add, remain_rate, period, create_time)
        ret_list.append(tup)

    logger.info(ret_list)

    dao = MysqlDao()
    dao.insertMany(sql, ret_list)

    logger.info('finished')

