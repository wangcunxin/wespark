# -*- coding: utf-8 -*-
import datetime, time, types, subprocess, os, sys

from logger import *

__author__ = 'kevin'


class ShellUtil:
    @staticmethod
    def callcheck(shell=''):
        result = True
        try:
            subprocess.check_call(shell, stdout=subprocess.PIPE, shell=True)
        except Exception as e:
            print(e)
            result = False
        return result


def execute(day):
    # chown
    hdfs_template = "su - hdfs -c 'hdfs dfs -chown -R impala:impala /output/site/site-sitePVv3/%s'"
    _sql = hdfs_template % day
    ret = ShellUtil.callcheck(_sql)

    txt_tb = "txtdb.site_sitepvv3"
    parquet_tb = "parquetdb.site_sitepvv3"
    impala_shell = "impala-shell -i slave0:21000 -q "

    # impala-shell -i cdh-slave0:21000 -q "select * from txtdb.site_sitepvv3 where dat='20151111'"
    # column_template = "%s 'describe %s'"
    add_partition_template = "%s \"alter table %s add if not exists partition (dat = '%s')\""
    drop_partition_template = "%s \"alter table %s drop if exists partition (dat = '%s')\""
    load_data_template = "%s \"load data inpath '%s' overwrite into table %s partition (dat = '%s')\""
    select_insert_template = "%s \"insert overwrite %s partition(dat ='%s') select %s from %s where dat ='%s'\""

    # columns
    columns = "site_id,site_uuid,site_uuid_ctime,ptitle,url," \
              "referrer,prevpid,attime,resolution,ip," \
              "ctime,language,cookie_enabled,ua,uuid," \
              "uuid_ctime,browser,os,tag_key,supp_id," \
              "gw_id,portal_version,from_page,channel_id,channel_list_id," \
              "content_id,advid,appid,spenttime,assingleaccess," \
              "asfirstaccess,aslastaccess"
    '''
    _sql = column_template % txt_tb
    cols = []
    rs = sql_context.sql(_sql)
    for r in rs:
        cols.append(r[1])
    columns = ','.join(cols)
    logger.debug(columns)
    '''
    # add partitions
    _sql = add_partition_template % (impala_shell, txt_tb, day)
    ret = ShellUtil.callcheck(_sql)
    _sql = add_partition_template % (impala_shell, parquet_tb, day)
    ret = ShellUtil.callcheck(_sql)

    # load data
    inpath = "/output/site/site-sitePVv3/%s" % day
    _sql = load_data_template % (impala_shell, inpath, txt_tb, day)
    ret = ShellUtil.callcheck(_sql)

    # insert
    _sql = select_insert_template % (impala_shell, parquet_tb, day, columns, txt_tb, day)
    ret = ShellUtil.callcheck(_sql)

    # drop partitions
    _sql = drop_partition_template % (impala_shell, txt_tb, day)
    ret = ShellUtil.callcheck(_sql)

    logger.info("complete to chown , load and insert:" + str(ret))


# impala-shell -i cdh-slave0:21000 -q/-f sql/file
if __name__ == '__main__':

    # --set datetime
    DAY_OFFSET = 1
    now = datetime.datetime.now()
    pro_time = now - datetime.timedelta(days=DAY_OFFSET)
    day = pro_time.strftime("%Y%m%d")

    '''
    if len(sys.argv) != 2:
        print("Usage: hdfs_impala.py <input=20151111>")
        sys.exit(-1)
    day = sys.argv[1]
    '''
    logger.info("start to handle:" + day)

    try:
        execute(day)
    except Exception, e:
        logger.error(e)
