from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils
from pyspark import SparkConf, SparkContext
import os

sep ="\t"
def my_print(l):
    print(l)

def parseLine(line):
    parts = line.split(sep)
    label = float(parts[0])
    features = Vectors.dense([float(x) for x in parts[1].split(',')])
    return LabeledPoint(label, features)

if __name__ == '__main__':
    day = '20160225'
    master = "local[*]"

    app_name = "ml_app"
    spark_home = '/opt/cloud/spark'
    os.environ['SPARK_HOME'] = spark_home

    input = "/input/ml_data/decisiontree.dat"
    output = "/output/ml_model/decisiontree_regression/dat=%s" % '20160226'

    conf = (SparkConf()
            .setMaster(master)
            .setAppName(app_name))

    sc = SparkContext(conf=conf)
    lines = sc.textFile(input)
    parsedData = lines.map(parseLine)
    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = parsedData.randomSplit([0.4, 0.6])

    # Train a DecisionTree model.
    #  Empty categoricalFeaturesInfo indicates all features are continuous.
    model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo={},
                                        impurity='variance', maxDepth=5, maxBins=32)

    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testMSE = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() / float(testData.count())
    print('mse = ' + str(testMSE))
    print('Learned regression tree model:')
    print(model.toDebugString())

    # Save and load model
    model.save(sc, output)
    sameModel = DecisionTreeModel.load(sc, output)

    sc.stop()