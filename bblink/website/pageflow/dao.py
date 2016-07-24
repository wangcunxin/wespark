import pymongo
import pymysql

# sftp,mysql,zookeeper,kafka:ip port username password
class ConfigAddr:

    # formal mysql
    '''
    mysql_host = '121.40.175.62'
    mysql_port = 3306
    mysql_username = 'bblink_data'
    mysql_password = 'ZTpDbEjpNI0vx32cFSft'
    mysql_db = 'bblink_data'
    '''

    # test mysql
    mysql_host = '192.168.0.173'
    mysql_port = 3306
    mysql_username = 'root'
    mysql_password = 'bblink2014$'
    mysql_db = 'bblink_usercenter'

class MysqlDao:
    def __init__(self):
        self.__config = {
            'host': ConfigAddr.mysql_host,
            'port': ConfigAddr.mysql_port,
            'username': ConfigAddr.mysql_username,
            'passwd': ConfigAddr.mysql_password,
            'db': ConfigAddr.mysql_db
        }

    def initConn(self):
        self.__conn = pymysql.connect(host=self.__config['host'],
                                      port=self.__config['port'],
                                      user=self.__config['username'],
                                      passwd=self.__config['passwd'],
                                      db=self.__config['db'])
        self.__cur = self.__conn.cursor()

    def findWithQuery(self, queryStr=None):
        self.initConn()
        self.__cur.execute(queryStr)
        res = self.__cur.fetchall()
        self.__cur.close()
        self.__conn.close()
        return res

    def findOne(self, queryStr=None):
        self.initConn()
        self.__cur.execute(queryStr)
        res = self.__cur.fetchone()
        self.__cur.close()
        self.__conn.close()
        return res

    def insert(self, insertStr=None):
        self.initConn()
        self.__cur.execute(insertStr)
        self.__cur.close()
        self.__conn.commit()
        self.__conn.close()

    def insertMany(self, insertStr='', list=None):
        self.initConn()
        self.__cur.executemany(insertStr, list)
        self.__cur.close()
        self.__conn.commit()
        self.__conn.close()

    def insertFormat(self, sql=None, data=None):
        self.initConn()
        self.__cur.execute(sql, data)
        self.__cur.close()
        self.__conn.commit()
        self.__conn.close()

    def update(self, updateStr=None):
        self.initConn()
        self.__cur.execute(updateStr)
        self.__cur.close()
        self.__conn.commit()
        self.__conn.close()

    def delete(self, deleteStr=None):
        self.initConn()
        self.__cur.execute(deleteStr)
        self.__cur.close()
        self.__conn.commit()
        self.__conn.close()

