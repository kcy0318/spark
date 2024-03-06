# -*- encoding: utf-8 -*-
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local[*]').setAppName('hello world')
sc = SparkContext(conf=conf)

# 方式1：通过读取外部数据集创建RDD
f1 = sc.textFile('d:/f.txt')
# 打印
f1.foreach(print)

# 方式2：通过集合创建RDD
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9]
s1 = sc.parallelize(numbers)
# 打印
s1.foreach(print)
