# -*- encoding = utf-8 -*-
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("student score") \
    .enableHiveSupport().getOrCreate()

# 设置matplotlib字体
plt.rcParams["font.sans-serif"] = ["KaiTi"]  # 设置字体
plt.rcParams["axes.unicode_minus"] = False  # 正常显示负号
plt.rcParams.update({'font.size': 16})

def _map_to_pandas(rdds):
    return [pd.DataFrame(list(rdds))]

def topas(df, n_partitions=None):
    if n_partitions is not None: df = df.repartition(n_partitions)
    df_pand = df.rdd.mapPartitions(_map_to_pandas).collect()
    df_pand = pd.concat(df_pand)
    df_pand.columns = df.columns
    return df_pand

# 读取有标题行的文件
options = {'header': 'True',
           'sep': ',',
           'inferSchema': 'True'}
dfStudentMat = spark.read \
    .options(**options) \
    .format('csv') \
    .load('E:\KCY\大学\教育大数据\student-mat.csv')
dfStudentMat.createOrReplaceTempView('mat')
# 选取G3属性值
pdMat = topas(dfStudentMat)
labels = pdMat['G3']
# 删除school，G1和G2属性
student = pdMat.drop(['school', 'G1', 'G2'], axis='columns')
# 对离散变量进行独热编码
student = pd.get_dummies(student)
# 选取相关性最强的6个
most_correlated = student.corr().abs()['G3'].sort_values(ascending=False)
most_correlated = most_correlated[:6]
print(most_correlated)
