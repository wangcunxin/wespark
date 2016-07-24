from __future__ import print_function
import os
import sys

import numpy as np
from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import KMeans
from bblink.jobs.cfilter.logger import *

sep ="\t"
def my_print(l):
    print(l)

def parseVector(line):
    a = line.split(sep)
    return [a[0]] + [float(x) for x in a[1:2]]

def prepare_ret(ar):
    size = len(ar)
    ret = []
    for i in range(0,size):
        ret.append(str(ar[i]))
    return sep.join(ret)

if __name__ == "__main__":
    '''
    if len(sys.argv) != 4:
        print("Usage: kmeans <day> <k>  <master>", file=sys.stderr)
        exit(-1)

    day = sys.argv[1]
    k = int(sys.argv[2])
    master = sys.argv[3]
    '''
    #file_test = sys.argv[3]
    #file_predict = sys.argv[4]
    k=10
    master='local[*]'
    input = '/output/uservisitinterval/dat=%s' % '20160220'
    output = '/output/uservisitinterval_ret/dat=%s' % '20160220'
    '''
    input = "/output/back/mid_uservisitinterval/dat=%s" % day
    output = "/output/back/ret_uservisitinterval/dat=%s" % day
    '''
    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home
    conf = (SparkConf()
            .setMaster(master)
            .setAppName("KMeans"))

    sc = SparkContext(conf=conf)
    lines = sc.textFile(input)
    data = lines.map(parseVector)
    data_train = data.map(lambda v: v[1:])
    #data_train.foreach(my_print)
    '''
    # search the best k
    str_ret = ''
    for k in range(1,10):
        model = KMeans.train(data_train, k, maxIterations=100, runs=10)
        #print('k='+str(k)+','+str(model.computeCost(data))+','+str(model.clusterCenters))
        str_ret+='k='+str(k)+','+str(model.computeCost(data_train))+'\r\n'

    print(str_ret)
    '''
    # train
    model = KMeans.train(data_train, k, maxIterations=100, runs=10)
    print('k=' + str(k) + ',' + str(model.computeCost(data_train)) + ',' + str(model.clusterCenters))

    ret_arr = []
    # predict
    for ar in data.collect():
        ret = model.predict(ar[1:])
        ret_arr.append(prepare_ret(ar) + sep + str(ret))

    rdd_ret = sc.parallelize(ret_arr)

    rdd_ret.saveAsTextFile(output)

    sc.stop()
