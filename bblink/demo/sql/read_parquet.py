from pyspark import SparkContext

__author__ = 'kevin'

from pyspark.sql import SQLContext

if __name__ == '__main__':
    master = "spark://hadoop:7077"
    appName = "sparkApp"
    #inputSrc ="har://hdfs-master:8020/user/impala/impaladir/kafka01/20150909.har/part-*"
    #inputSrc ="har://master:8020/user/impala/impaladir/kafka01/20150909.har/part-*"
    inputSrc ="/output/kafka/20150921"

    sc = SparkContext(master, appName)
    sql_context = SQLContext(sc)

    #parquet_file = sql_context.read.parquet(inputSrc)
    parquet_file = sql_context.read.format('parquet').load(inputSrc)

    parquet_file.registerTempTable("word_count")
    words_count_sql_df = sql_context.sql("select word,total from word_count")

    for word,total in words_count_sql_df.collect():
        print word,total

    sc.stop()
