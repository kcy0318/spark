# -*- encoding = utf-8 -*-
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf.setMaster('local[*]').setAppName('hello world')
sc = SparkContext(conf=conf)
# 构建SparkSession对象
spark = SparkSession.builder.master('local[*]').appName('demo').enableHiveSupport().getOrCreate()
# 定义选项
options = {'header': 'True', 'inferSchema': 'True', 'sep': ',', 'emptyValue': '0'}
# 加载数据集
deptDF = spark.read.options(**options).format('csv').load('data/dept.csv')
# 显示数据
deptDF.show()
# 显示结构
deptDF.printSchema()
# 探索数据
deptDF.describe('deptName').show()
# 缓存数据
deptDF.cache()
# 生成全局或临时视图
deptDF.createOrReplaceTempView('dept')
# 使用全局或临时视图
deptDFOfView = spark.sql("SELECT deptNO, deptName, address FROM dept")
# 显示数据
deptDFOfView.show()

# 加载数据集
empDF = spark.read.options(**options).format('csv').load('data/emp.csv')
# 显示数据
empDF.show()
# 显示结构
empDF.printSchema()
# 数据探索
empDF.describe('salary').show()
# 数据缓存
empDF.cache()
# 生成全局或局部视图
empDF.createOrReplaceGlobalTempView('emp')
# 使用全局或局部视图
empDFOfView = spark.sql("selcet empNO, empName, station, MGR, hiredate, salary, bonus, deptNO FROM emp")
# 显示数据
empDFOfView.show()

# 多表查询：查看员工的详细信息，包括部门名称、地址
detailsDF = spark.sql("SELECT d.deptNO, deptName, address, empNO, empName, station, MGR, hiredate, salary, bonus "
                      "FROM dept d "
                      "inner join emp e "
                      "on d.deptNO = e.deptNO")
# 显示员工详细信息
detailsDF.show()
# 生成全局或临时视图
detailsDF.createOrReplaceTempView('details')