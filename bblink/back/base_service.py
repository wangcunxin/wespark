from bblink.back.logger import *
from bblink.back.dao import MysqlDao

__author__ = 'kevin'

# execute sql and save to db
class BaseService:

    # mysql
    def write_mysql(self, list, delete_sql, insert_sql):

        if list != None and len(list) > 0:
            dao = MysqlDao()

            try:
                logger.debug(delete_sql)

                dao.delete(delete_sql)
                dao.insertMany(insert_sql, list)
            except Exception, e:
                logger.error(e)

    # mysql:insert and update
    def write_mysql_insert_update(self, insert_sql,insert_list, update_sql,update_list):

        if insert_list != None and len(insert_list) > 0 and update_list != None and len(update_list) > 0:
            dao = MysqlDao()
            try:
                logger.debug(insert_sql)
                logger.debug(len(insert_list))
                logger.debug(insert_list[0])
                logger.debug(update_sql)
                logger.debug(len(update_list))
                logger.debug(update_list[0])
                dao.insertMany(insert_sql, insert_list)
                dao.insertMany(update_sql, update_list)
            except Exception, e:
                logger.error(e)