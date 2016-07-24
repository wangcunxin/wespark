# encoding: utf-8
__author__ = 'lidl'

import os
import sys

import datetime
import time

from pyspark import SparkContext,SparkConf

reload(sys)
sys.setdefaultencoding('utf8')

def dateConvLongtime( date):
    if date != None and date != '' and date != 'None':
        s = time.mktime(time.strptime(date, "%Y-%m-%d %H:%M:%S"))
        return s
    else:
        return getTime_format()

def getTime_format():
    return datetime.datetime.strptime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
def longTime2str(longtime=0):
    return datetime.datetime.fromtimestamp(long(longtime/1000)).strftime('%Y%m%d')
def printx(x):
    print(x)

def checkx(x):
    return x
def removeNight(x):
    list=x[1]
    flag=True
    for item in list:
        nighttime= datetime.datetime.fromtimestamp(long(item)/1000).strftime('%H:%M:%S')
        if nighttime>'17:00:00' or nighttime<'07:00:00':
            flag= False
            break
    return flag
def removeInWeekOrNeighborWeek(x):
    _list=x[1]
    # remove login time ==1
    if len(_list)==1:
        return False
    # weekday=[]
    _dict={} #week,weekday_list
    for item in _list:
        time= datetime.datetime.fromtimestamp(int(int(item)/1000))
        # weekday.append((time.isocalendar()[1],time.weekday()))
        weekIndex=time.isocalendar()[1]
        weekDayIndex=time.weekday()
        if _dict.get(weekIndex)!=None:
            listByWeek=_dict[weekIndex]
            listByWeek.append(weekDayIndex)
        else:
            _dict[weekIndex]=[weekDayIndex]
    # check week neighbor

    # distinct list in dict
    for key in _dict:
        _dict[key]=list(set(_dict[key]))

    # print key ,_dict
    # print(x[0])
    # print(_dict)

    # key to num for iterator neighbor week check
    weekNum=[]
    for key in _dict:
        weekNum.append(key)
    weekNum=sorted(weekNum)
    index=0
    for num in weekNum:
        try:
            # print "in for loop:"+str((weekNum[index])+weekNum[index+2])+"=="+str(weekNum[index+1])
            # print(index)
            if (weekNum[index] + weekNum[index+2])==weekNum[index+1]*2:
            # if weekNum[index+1] + weekNum[index+2]==weekNum[index]+weekNum[index+3]:
            #     print("yes")
                return False
                break
        except:
            pass
        # check logintime in one week if gt 3
        if len(_dict[num])>3:
            return False
            break
        index=index+1
    return True


if __name__ == '__main__':

    argArray=sys.argv
    spark_home = '/home/lidl/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    logFile ='hdfs://cdh-master:8020/logs_origin/back/back-portal-loginlog/201510*0,hdfs://cdh-master:8020/logs_origin/back/back-portal-loginlog/201510*1'

    conf = (SparkConf()
         .setMaster("local[2]")
         .setAppName("user-filter")
         .set("spark.kryoserializer.buffer.mb", "256"))
    sc = SparkContext(conf = conf)

    limitList=['61060300FF0C956E','50EB974664','242A80CA05','001631f1b920','61060300FF0C2F82','61060300FF0C6ADC','001631f1ba14','2e13f500e7d18b2a65447fe81d38911d','61060300FF0CA3B5','61060300FF0C27B0']
    '''
    logic:
    1\ remove nightdata >17:00:00 or < 07:00:00
    2\ login in 4 neighbor weeks
    3\ login times >3 in one week
    '''

    '''
    tupleList=[
        ('10',('13700000001',1448861897000)),# 20151130 13:38:17 w49
        ('11',('13800000001',1448877600000)),# 20151130 18:00:00 w49
        ('3',('(13300000001',1448827200000)),# 20151130 04:00:00 w49


        ('25',('13300000006',1447552800000)),# 20151115 10:00:00 w46
        ('25',('13300000006',1447552800000)),# 20151115 10:02:00 w46
        ('25',('13300000006',1447556400000)),# 20151115 11:00:00 w46
        ('25',('13300000006',1447653600000)),# 20151116 14:00:00 w47
        ('25',('13300000006',1447642800000)),# 20151116 11:00:00 w47

        ('25',('13300000002',1447736400000)),# 20151117 13 w47 -- in weeks test--
        # ('15',('13300000002',1447822800000),# 20151118 13 w47
        ('25',('13300000002',1447909200000)),# 20151119 13 w47
        ('25',('13300000002',1447995600000)),# 20151120 13 w47
        ('25',('13300000002',1448168400000)),# 20151122 13 w48

        ('25',('13300000003',1446613200000)),# 20151104 13 w45 --neighbor weeks test--
        ('25',('13300000003',1447131600000)),# 20151110 13 w46
        ('25',('13300000003',1447995600000)),# 20151120 13 w47
        ('25',('13300000003',1444194000000)) # 20151007 13 w41
    ]

    _rdd=sc.parallelize(tupleList).filter(lambda x:str(x[0]) in limitList).values()
    _rdd.foreach(printx)
    '''
#gwid,no,time
    _rdd = sc.textFile(logFile)\
        .map(lambda line: (line.split('\t')[2],(line.split('\t')[4], line.split("\t")[5])))\
        .filter(lambda x:str(x[0]) in limitList).values()\
        .filter(lambda x:x[0]!='')\
        .filter(lambda x:x[0]!='null')\
        .filter(lambda x:len(x[1])==13)\
        .filter(lambda x:len(x[0])==11)

    # resultData.saveAsTextFile('file:///tmp/result.csv')

    resultData=_rdd.groupByKey()\
        .filter(removeNight)\
        .filter(removeInWeekOrNeighborWeek).keys()
    resultData.saveAsTextFile(
        "hdfs://cdh-master:8020/tmp/userfilter2")
    sc.stop()


