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
# 学生自己的升学意志对成绩的影响
student = topas(spark.sql('select higher,G3 from mat'))
personal_wish = sns.boxplot(x=student['higher'], y=student['G3'])
personal_wish.axes.set_title('学生升学意愿对成绩的影响', fontsize=20)
personal_wish.set_xlabel('更高级的教育 (1 = 是)', fontsize=12)
personal_wish.set_ylabel('最终成绩', fontsize=12)
plt.show()
