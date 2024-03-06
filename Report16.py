# -*- encoding: utf-8 -*-
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession

# 设置matplotlib字体
sns.set(font='SimHei')
plt.rcParams.update({'font.size': 8})
plt.rcParams["font.sans-serif"] = ["SimHei"]  # 设置字体
plt.rcParams["axes.unicode_minus"] = False  # 正常显示负号


# 由于pandas的方式是单机版的，即toPandas()的方式是单机版的
# 下述2个方法，是将Spark DataFrame 转换为 Pandas
def _map_to_pandas(rdds):
    return [pd.DataFrame(list(rdds))]


def topas(df, n_partitions=None):
    if n_partitions is not None: df = df.repartition(n_partitions)
    df_pand = df.rdd.mapPartitions(_map_to_pandas).collect()
    df_pand = pd.concat(df_pand)
    df_pand.columns = df.columns
    return df_pand


spark = SparkSession.builder.master("local[*]").appName("student score") \
    .enableHiveSupport().getOrCreate()
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
# dfStudentMat.describe().show()
# G3属性分析
# dfStudentMat.describe('G3').show()

# pyspark机器学习
print('-------------pyspark机器学习--------------------------')
# 特征列编码
enc_cols = ['school', 'sex', 'age', 'address', 'famsize', 'Pstatus', 'Medu', 'Fedu', 'Mjob',
            'Fjob', 'reason', 'guardian', 'traveltime', 'studytime', 'failures', 'schoolsup',
            'famsup', 'paid', 'activities', 'nursery', 'higher', 'internet', 'romantic',
            'famrel', 'freetime', 'goout', 'Dalc', 'Walc', 'health', 'absences', 'G3']
enc_cols_ind = ['{}_ind'.format(s) for s in enc_cols]
enc_cols_val = ['{}_val'.format(s) for s in enc_cols]
# 一次性编码
stringindexer = StringIndexer(inputCols=enc_cols, outputCols=enc_cols_ind)
onehotenc = OneHotEncoder(inputCols=enc_cols_ind, outputCols=enc_cols_val).setHandleInvalid("keep")
pipeline = Pipeline(stages=[stringindexer, onehotenc])
pipeline_fit = pipeline.fit(dfStudentMat)
enc_df = pipeline_fit.transform(dfStudentMat)
# 将多个列合并为向量列的特征转换器
v = VectorAssembler().setInputCols(enc_cols_val).setOutputCol('features')
features_df = v.transform(enc_df)
# 查看变换后的结构
features_df.printSchema()
# 构建用于线性回归的数据模型
model_df = features_df.select('features', 'G3')
# 将数据划分为 训练数据和预测数据
train_df, test_df = model_df.randomSplit([0.7, 0.3])
# 构建线性回归模型, labelCol,相对于featrues列，表示要进行预测的列
lin_Reg = LinearRegression(labelCol='G3')
# 训练数据
lr_model = lin_Reg.fit(train_df)
# intercept 线性方程的截距
print('{}{}'.format('方程截距:', lr_model.intercept))
# 回归方程中的，变量参数
print('{}{}'.format('方程参数系数:', lr_model.coefficients))
# 查看预测数据
training_predictions = lr_model.evaluate(train_df)
# 误差值差值平方
print('{}{}'.format('误差差值平方:', training_predictions.meanSquaredError))
# r2 判定系数,用来判定，构建的模型是否能够准确的预测,越大说明预测的准确率越高
print('{}{}'.format('判定系数：', training_predictions.r2))
# 使用预测数据,用已经到构建好的预测模型 lr_model
test_results = lr_model.evaluate(test_df)
# 查看预测的拟合程度
print('拟合度：{}'.format(test_results.r2))
# 查看均方误差
print('均方误差：{}'.format(test_results.meanSquaredError))
# 保存模型
lr_model.save('pyspark_lr_model')

