__author__ = 'kevin'

class ConfigSpark:
    master = "local[1]"
    #master = "spark://master:7077"
    separator = "\t"


# sftp,mysql,zookeeper,kafka:ip port username password
class ConfigAddr:

    # zookeeper
    zk_quorum = 'slave0:2181,slave1:2181,slave2:2181'
    # kafka
    brokers = 'kafka1:9092,kafka2:9092,kafka3:9092'

    '''
    # sftp addr
    host = '112.65.205.87'
    port = 22
    user_name = 'root'
    password = 'bblink2014!'
    dest_dir = "/home/hadoop/data/download/xxx/"
    '''

    mysql_host = '112.65.205.87'
    mysql_port = 3306
    mysql_username = 's_bblink_hos'
    mysql_password = 'A719200F0BF4416B'
    mysql_db = 'bblink_hos'

    '''
    # intranet ip
    mysql_host = '192.168.100.102'
    mysql_port = 3306
    mysql_username = 'hoswifi'
    mysql_password = 'hoswifi_password'
    mysql_db = 'bblink_hos'


    # test mysql
    mysql_host = '192.168.0.173'
    mysql_port = 3306
    mysql_username = 'bblink_hos'
    mysql_password = 'bblink_hos'
    mysql_db = 'bblink_hos'
    '''

#  path:output,checkpoint,cache
class ConfigSparkPath:

    __base_path = "hdfs://master:8020"
    __input = __base_path+"/logs_origin"
    __output = __base_path+"/output"

    input_sitepvv3 = __input + "/site/site-sitePVv3/%s"
    output_sitepvv3 = __output+"/site/site-sitePVv3/%s"

