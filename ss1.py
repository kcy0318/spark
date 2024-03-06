# -*- encoding = utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode

# 构建SparkSession对象
spark = SparkSession.builder.master('local[*]').appName('demo').enableHiveSupport().getOrCreate()
# 定义输入流
lines = spark.readStream.format("socket").options('host', 'localhost').options('port', 1688).load()
# 转换操作
words = lines.select(explode(split(lines.value, ' ')).alias('word'))
# 聚合计算
wc = words.groupBy('word').count()
# 输出
output = wc.writeStream.outputMode('complete').format('console').start()
# 等待结束
output.awaitTermination()