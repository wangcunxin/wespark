from bblink.jobs.repeatlogin import logger
from bblink.jobs.repeatlogin.dao import MysqlDao

__author__ = 'kevin'

# execute sql and save to db
class BaseService:
    def __init__(self):
        pass

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
                fo.write(line+'\n')

        except Exception, e:
            print(e.__str__())
        finally:
            try:
                if fo != None:
                    fo.close()
            except Exception, e:
                print(e.__str__())

    # file:hdfs://namenode:port/
    def _write_hdfs_file(self, list, output_path):
        pass

    # mysql
    def _write_mysql(self, list, delete_sql, insert_sql):

        if list != None and len(list) > 0:
            dao = MysqlDao()

            try:
                logger.debug(delete_sql)

                dao.delete(delete_sql)
                dao.insertMany(insert_sql, list)
            except Exception, e:
                logger.error(e.__str__())