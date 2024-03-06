# -*- encoding: utf-8 -*-
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local[*]').setAppName('hello world')
sc = SparkContext(conf=conf)

# 转换操作与行动操作1
f1 = sc.textFile('d:/f.txt')
# 转换操作
f2 = f1.filter(lambda line: 'china' in line)
# 行动操作
count = f2.count()
# 打印
print(count)

# 转换操作与行动操作2
s1 = sc.textFile('d:/f.txt')
# 转换操作
s2 = s1.map(lambda line: line.split(' '))
# 行动操作
count = s2.count()
# 打印
print(count)

