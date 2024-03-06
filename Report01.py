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
# dfStudentMat.show()
# dfStudentMat.printSchema()
# 使用图表分析属性
# 根据人数多少统计各分数段的学生人数
# grade_counts = topas(dfStudentMat)['G3'].value_counts().sort_values().plot.barh(width=.9,color=sns.color_palette('inferno',40))
grade_counts = topas(dfStudentMat.select('G3'))['G3'].value_counts().sort_values().plot.barh(width=.9,
                                                                                             color=sns.color_palette(
                                                                                                 'inferno', 40))
grade_counts.axes.set_title('各分数值的学生分布', fontsize=24)
grade_counts.set_xlabel('学生数量', fontsize=24)
grade_counts.set_ylabel('最终成绩', fontsize=24)
plt.show()
