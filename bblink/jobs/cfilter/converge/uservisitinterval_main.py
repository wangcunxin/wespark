from __future__ import print_function

import sys,os
from datetime import datetime

from pyspark import SparkConf, SparkContext, SQLContext

sep ="\t"
def my_print(l):
    print(l)

def filter_invalid_time(r):
    return str(r[0])>='20140101'

def subStr(s):
    return s[8:10]

def convert_kv(r):
    #return (r[1],subStr(r[3]))
    return (r[1],r[0])
def convert_kv_2(r):
    return (r[1],subStr(r[3]))
def convert_kv_3(r):
    return (r[1],subStr(r[4]))

def convert_min(kvs):
    size = len(kvs[1])
    return (kvs[0],int(kvs[1][0]))
def convert_max(kvs):
    size = len(kvs[1])
    return (kvs[0],int(kvs[1][size-1]))

def convert_set(kvs):
    vs = set(kvs[1])
    vs_tmp = list(vs)
    list.sort(vs_tmp)
    return (kvs[0],vs_tmp)

def datediff(a1,a2):
    d1 = datetime.datetime(int(a1[0:4]),int(a1[4:6]),int(a1[6:8]))
    d2 = datetime.datetime(int(a2[0:4]),int(a2[4:6]),int(a2[6:8]))
    return (d2-d1).days

def compute_days(beginDate,endDate):
    format="%Y%m%d";
    bd=datetime.strptime(beginDate,format)
    ed=datetime.strptime(endDate,format)
    diff = ed-bd
    return diff.days

def calculate_sum(vs):
    sum=0
    size = len(vs)
    for i in range(0,size-1):
         begin= vs[i]
         end= vs[i+1]
         sum+=compute_days(begin,end)
    return sum

def get_distinct_list(ds,diff):
    ret = []
    tmp=ds[0]
    _list=[]

    for d in ds:
        _diff=compute_days(tmp,d)
        if _diff<=diff:
            _list.append(d)
            tmp = d
        else:
            ret.append(_list)
            _list=[]
            tmp=d
            _list.append(d)

    ret.append(_list)
    return ret

def getMedian(a):
    ret =0
    size = len(a)
    mid = size%2
    if mid==0:
        #ret = (a[size/2]+a[size/2-1])/2
        ret = a[size/2-1]
    elif mid==1:
        ret = a[(size-1)/2]
    return ret

def get_median_list(ars):
    ret = []
    for ar in ars:
        m = getMedian(ar)
        ret.append(m)
    return ret

def listSum(ar):
    ret =0
    size = len(ar)
    for i in range(0,size-1):
        ret +=compute_days(ar[i],ar[i+1])
    return ret

def get_avg_diff(ar):
    ret =0
    size = len(ar)
    if size==2:
        ret = compute_days(ar[0],ar[1])
    elif size>=3:
        ret = listSum(ar)/(size-1)
    return ret

def convert_median_avg(kv):
    ret = 0
    # 1,2,4,7=[[1,2,4],[7]]
    arr = get_distinct_list(kv[1],1)
    # medians
    medians = get_median_list(arr)
    # medians avg
    ret = get_avg_diff(medians)

    return (kv[0],ret)

def convert_count(kvs):

    return (kvs[0],len(kvs[1]))

def count_interval(kvs):
    #(u'582AF72F2873', [u'20151201', u'20151220'])
    k = kvs[0]
    vs = kvs[1]
    count=len(vs)
    ret = ()
    if count==1:
        ret = (k,0)
    elif count>1:
        count = count-1
        sum_interval = calculate_sum(vs)
        ret = (k,sum_interval/count)
    else :
        ret = (k,0)
    return ret

def prepare_ret(kvs):
    #vs = kvs[1]
    ret = [kvs[0]]
    for v in kvs[1]:
        ret.append(str(v))
    return (None, sep.join(ret))

def convert_vs(kvs):
    #(u'000AF55942AC', (((0, 1), 9), 9))
    a=kvs[1]
    b=a[0]
    c=b[0]
    return (kvs[0],[c[0],c[1],b[1],a[1]])

__author__ = 'kevin'
'''
compute user_mac and interval_avg:median_interval_sum/(count-1)
(median-median)/(len-1)
'''
if __name__ == "__main__":
    '''
    if len(sys.argv) != 3:
        print("Usage: uservisitinterval_main <day> <master>", file=sys.stderr)
        exit(-1)

    day = sys.argv[1]
    master = sys.argv[2]
    '''
    day = '20160220'
    master = "local[*]"

    app_name = "uservisitinterval_app"
    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home
    '''
    input = "/impala/parquet/back/mid_uservisittime_day/dat=%s/*" % '*'
    output = "/output/back/mid_uservisitinterval/dat=%s" % day
    '''
    input = "/output/mid_uservisittime_day/dat=%s" % '2016*'
    output = "/output/uservisitinterval/dat=%s" % day

    conf = (SparkConf()
            .setMaster(master)
            .setAppName(app_name))

    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # mac,login_time
    df = sqlContext.read.parquet(input)
    rdd0 = df.rdd.filter(filter_invalid_time)

    rdd1 = rdd0.map(convert_kv)\
        .groupByKey().mapValues(list).map(convert_set)
    #rdd1.foreach(my_print)

    # mac:median_avg
    rdd2_1 = rdd1.map(convert_median_avg)
    #rdd2_1.foreach(my_print)

    # mac:count
    rdd2_2 = rdd1.map(convert_count)
    #rdd2_2.foreach(my_print)

    # mac:begin,end
    rdd2_3 = rdd0.map(convert_kv_2)\
        .groupByKey().mapValues(list).map(convert_set)\
        .map(convert_min)
    rdd2_4 = rdd0.map(convert_kv_3)\
        .groupByKey().mapValues(list).map(convert_set)\
        .map(convert_max)
    '''
    rdd3 = rdd2_1.union(rdd2_2).union(rdd2_3).union(rdd2_4)\
        .groupByKey().mapValues(list)
    '''
    rdd3 = rdd2_1.join(rdd2_2).join(rdd2_3).join(rdd2_4).map(convert_vs)
    #rdd3.foreach(my_print)

    #avg,count,min,max
    #(u'B0AA36D0C05E', [0, 1, u'08', u'14'])
    rdd4 = rdd3.filter(lambda kvs:kvs[1][0]!=0)\
        .map(lambda kvs:prepare_ret(kvs)).values()\
        .repartition(10)
    #rdd4.foreach(my_print)
    #rdd4.takeSample(1,1)

    rdd4.saveAsTextFile(output)

    sc.stop()