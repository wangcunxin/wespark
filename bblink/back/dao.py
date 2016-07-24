import pymongo
import pymysql

# sftp,mysql,zookeeper,kafka:ip port username password
class ConfigAddr:
    # test mysql
    mysql_host = '192.168.0.173'
    mysql_port = 3306
    mysql_username = 'bblink_hos'
    mysql_password = 'bblink_hos'
    mysql_db = 'bblink_data'


    # mongodb
    mongodb_host ="192.168.100.103,192.168.100.104,192.168.100.111"
    mongodb_port =30000
    mongodb_username ='bblink_logs'
    mongodb_password ="Bblink#2015$"
    mongodb_dbname ='bblinklogs'

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


class MongoDao:
    def __init__(self):
        self.__host = ConfigAddr.mongodb_host
        self.__port = ConfigAddr.mongodb_port
        self.__username = ConfigAddr.mongodb_username
        self.__password = ConfigAddr.mongodb_password
        self.__dbname = ConfigAddr.mongodb_dbname

    def use_mangodb_client(self):
        try:
            self.__mongo = pymongo.MongoClient(self.__host, self.__port)
            self.__db = self.__mongo.get_default_database()

        except pymongo.errors.ConnectionFailure, e:
            print "Could not connect to MongoDB: %s" % e

    def useCollection(self, collectionName=None):
        self.__collection = self.__db[collectionName]

    def findWithQuery(self, query=None, skip=None, limit=None):
        if skip != None and limit != None:
            return self.__collection.find(query).skip(0).limit(20)
        else:
            return self.__collection.find(query)

    def findAll(self):
        return self.__collection.find()

    def findOne(self, query=None):
        return self.__collection.find_one(query)

    def insert(self, obj=None):
        return self.__collection.insert(obj)
