# -*- encoding = utf-8 -*-
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local[*]').setAppName('hello world')
sc = SparkContext(conf=conf)
fileRDD = sc.textFile('d:/f.txt', 5)
#输出第一行文本
print(fileRDD.first())
#输出总行数
print(fileRDD.count())