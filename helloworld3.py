# -*- encoding = utf-8 -*-
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local[*]').setAppName('hello world')
sc = SparkContext(conf=conf)
# 原始数据集
rdd1 = sc.textFile('d:/f.txt', 5)

# 定义一个函数：检查一行文本是否包含china
def checkChina(line: str) -> bool:
    return 'china' in line

# 转换操作：返回包含china的行
rdd2 = rdd1.filter(checkChina)
# 持久化：以后多次使用
rdd2.cache()
# 行动操作：统计行数
count = rdd2.count()
# 输出结果
print(count)