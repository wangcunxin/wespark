from __future__ import print_function
import os

from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SparkConf, SparkContext


sep ="\t"
def my_print(l):
    print(l)

if __name__ == '__main__':

    day = '20160225'
    master = "local[*]"

    app_name = "uservisitinterval_app"
    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    input = "/input/ml_data/cfilter.data"
    output = "/output/ml_model/cfilter/dat=%s" % '20160225'

    conf = (SparkConf()
            .setMaster(master)
            .setAppName(app_name))

    sc = SparkContext(conf=conf)
    # Load and parse the data
    data = sc.textFile(input)
    ratings = data.map(lambda l: l.split(',')).filter(lambda a:len(a)==4)\
        .map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

    # Build the recommendation model using Alternating Least Squares
    rank = 10
    numIterations = 20
    model = ALS.train(ratings, rank, numIterations)

    # Evaluate the model on training data
    testdata = ratings.map(lambda p: (p[0], p[1]))

    predictions = model.predictAll(testdata)\
        .map(lambda r: ((r[0], r[1]), r[2]))
    predictions.foreach(my_print)

    ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
    print("mse = " + str(MSE)+", model="+output)

    # Save and load model
    model.save(sc, output)
    resume_model = MatrixFactorizationModel.load(sc, output)
    preds = resume_model.predictAll(testdata)\
        .map(lambda r: ((r[0], r[1]), r[2]))
    preds.foreach(my_print)

    sc.stop()