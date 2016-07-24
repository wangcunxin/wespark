import sys

from impala.dbapi import connect

from bblink.jobs.loaddata import logger

__author__ = 'kevin'
# impyla needs to upgrade python2.7,pause
if __name__ == '__main__':

    '''
    0.input date
    1.assemble file path to get file list
    2.get impala conn,tab columns
    3.load data inpath 'path' into table 'tb_txt' partition(dat = 'ymd')
    4.insert into tb_parquet partition(dat ='ymd') select * from tb_txt where dat ='ymd';
    '''

    if len(sys.argv) != 2:
        print("Usage: load_data_impala.py <input>")
        sys.exit(-1)

    #input_date = sys.argv[1:]
    date = sys.argv[1]
    logger.debug(date)

    input_path = "hdfs://cdh-master:8020/output/site/%s/%s"

    topic_list_site = ["site-sitePVv3"]
    tabs_list_site = ["site_sitePVv3"]
    logger.debug(topic_list_site)
    logger.debug(tabs_list_site)

    # get impala conn
    conn = connect(host="master", port=21050)
    cursor = conn.cursor()
    _sql = ""
    cursor.execute(_sql)
    data = cursor.fetchall()
    print data

    columns_list = [metadata[0] for metadata in cursor.description]
    logger.debug(columns_list)
    columns = ",".join(columns_list)

    # load data
    load_data = "load data inpath %s into table %s partition(dat = '%s')"
    select_insert="insert into %s partition(dat ='%s') select %s from %s where dat ='%s'"
    db_txt="txtdb."
    db_parquet="parquetdb."
    for topic in topic_list_site:
        path = input_path % (topic,date)
        logger.debug(path)
        for tab in tabs_list_site:
            load_sql = load_data % (path,tab,date)
            insert_sql = select_insert % (db_parquet+tab,date,columns,db_txt+tab,date)
            logger.debug(load_sql)
            logger.debug(insert_sql)
            cursor.execute(load_sql)
            cursor.execute(insert_sql)

    cursor.close()
    conn.close()
