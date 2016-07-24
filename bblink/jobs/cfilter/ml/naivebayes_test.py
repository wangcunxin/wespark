from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkConf, SparkContext
import os

sep ="\t"
def my_print(l):
    print(l)

def parseLine(line):
    parts = line.split(',')
    label = float(parts[0])
    features = Vectors.dense([float(x) for x in parts[1].split(sep)])
    return LabeledPoint(label, features)

if __name__ == '__main__':

    day = '20160225'
    master = "local[*]"

    app_name = "ml_app"
    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    input = "/input/ml_data/naivebayes.data"
    output = "/output/ml_model/naivebayes/dat=%s" % '20160226'

    conf = (SparkConf()
            .setMaster(master)
            .setAppName(app_name))

    sc = SparkContext(conf=conf)
    # Load and parse the data
    data = sc.textFile(input).map(parseLine)

    # Split data aproximately into training (60%) and test (40%)
    training, test = data.randomSplit([0.6, 0.4], seed = 0)

    # Train a naive Bayes model.
    model = NaiveBayes.train(training, 1.0)

    # Make prediction and test accuracy.
    predictionAndLabel = test.map(lambda p : (model.predict(p.features), p.label))
    predictionAndLabel.foreach(my_print)

    accuracy = 1.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / test.count()
    print("accuracy="+str(accuracy))

    model.save(sc,output)

    sc.stop()