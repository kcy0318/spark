# -*- encoding: utf-8 -*-
# -*- encoding: utf-8 -*-
from pyspark import SparkContext, SparkConf, StorageLevel

conf = SparkConf().setMaster('local[*]').setAppName('hello world')
sc = SparkContext(conf=conf)

# 方式1：通过读取外部数据集创建RDD
f1 = sc.textFile('d:/f.txt')
# 转换操作
f2 = f1.filter(lambda line: 'china' in line)
# 默认持久化
# 默认调用persist(StorageLevel.MEMORY_ONLY)
# f2.cache()
# 指定持久化：优先缓存至内存，内存不足时，缓存至磁盘
f2.persist(StorageLevel.MEMORY_AND_DISK)
# 行动操作：打印
f2.foreach(print)
# 转换操作
f3 = f2.filter(lambda line: line.split(' '))
# 行动操作
count = f3.count()
print(count)

