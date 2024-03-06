# -*- encoding = utf-8 -*-
from pyspark import  SparkConf, SparkContext
from pyspark.streaming import  StreamingContext

# 构建StreamingContext对象
conf = SparkConf()
conf.setAppName('file word count')
conf.setMaster('local[2]')
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 2)
# 定义输入流
lines = ssc.textFileStream('d:/000')
# 转换操作
words = lines.flatMap(lambda line: line.split(' '))
# 转换操作
wc1 = words.map(lambda w: (w,1))
# 转换操作
wc2 = wc1.reduceByKey(lambda  x, y: x+y)
# 行动操作
wc2.pprint()
# 启动
ssc.start()
# 等待结束
ssc.awaitTermination()