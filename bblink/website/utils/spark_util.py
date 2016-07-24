from pyspark import SQLContext

__author__ = 'kevin'


class SparkUtil:
    def __init__(self):
        pass

    def getSqlContextInstance(self,sparkContext):
        if ('sqlContextSingletonInstance' not in globals()):
            globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
        return globals()['sqlContextSingletonInstance']
