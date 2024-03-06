# -*- encoding: utf-8 -*-
from pyspark import SparkContext, SparkConf, StorageLevel

conf = SparkConf().setMaster('local[*]').setAppName('hello world')
sc = SparkContext(conf=conf)

# 指定分区数
f1 = sc.textFile('d:/f.txt', 4)
# 转换操作
f2 = f1.filter(lambda line: 'china' in line)
# 行动操作：打印
f2.foreach(print)

# 指定分区数
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9]
s1 = sc.parallelize(numbers, 2)
# 打印
s1.foreach(print)
