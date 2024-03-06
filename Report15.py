# -*- encoding: utf-8 -*-
import numpy as np
import pandas as pd
import pyspark
import seaborn as sns
from matplotlib import pyplot as plt
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.sql import SparkSession
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import ElasticNet
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.svm import SVR
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, median_absolute_error
import scipy
import pickle

# 设置matplotlib字体
sns.set(font='SimHei')
plt.rcParams.update({'font.size': 8})
plt.rcParams["font.sans-serif"] = ["KaiTi"]  # 设置字体
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
dfStudentMat.printSchema()
# dfStudentMat.describe().show()
# G3属性分析
dfStudentMat.describe('G3').show()
# 数据集分割
# ddd = dfStudentMat.fillna('0')
# ddd.show()
student = topas(dfStudentMat)
student = pd.get_dummies(student)
labels = student['G3']
X_train, X_test, y_train, y_test = train_test_split(student, labels, test_size=0.25, random_state=42)


# 训练之前的准备工作
# 计算平均绝对误差和均方根误差
# MAE-平均绝对误差
# RMSE-均方根误差
def evaluate_predictions(predictions, true):
    mae = np.mean(abs(predictions - true))
    rmse = np.sqrt(np.mean((predictions - true) ** 2))
    return mae, rmse


# 求中位数
median_pred = X_train['G3'].median()
# 所有中位数的列表
median_preds = [median_pred for _ in range(len(X_test))]
# 存储真实的G3值以传递给函数
true = X_test['G3']
# 展示基准
mb_mae, mb_rmse = evaluate_predictions(median_preds, true)
print('Median Baseline  MAE: {:.4f}'.format(mb_mae))
print('Median Baseline RMSE: {:.4f}'.format(mb_rmse))


# 通过训练集训练和测试集测试来生成多个线性模型
def evaluate(X_train, X_test, y_train, y_test):
    # 模型名称
    model_name_list = ['Linear Regression', 'ElasticNet Regression',
                       'Random Forest', 'Extra Trees', 'SVM',
                       'Gradient Boosted', 'Baseline']
    X_train = X_train.drop('G3', axis='columns')
    X_test = X_test.drop('G3', axis='columns')
    # 实例化模型
    model1 = LinearRegression()
    model2 = ElasticNet(alpha=1.0, l1_ratio=0.5)
    model3 = RandomForestRegressor(n_estimators=100)
    model4 = ExtraTreesRegressor(n_estimators=100)
    model5 = SVR(kernel='rbf', degree=3, C=1.0, gamma='auto')
    model6 = GradientBoostingRegressor(n_estimators=50)
    # 结果数据框
    results = pd.DataFrame(columns=['mae', 'rmse'], index=model_name_list)
    # 每种模型的训练和预测
    for i, model in enumerate([model1, model2, model3, model4, model5, model6]):
        model.fit(X_train, y_train)
        predictions = model.predict(X_test)
        # 误差标准
        mae = np.mean(abs(predictions - y_test))
        rmse = np.sqrt(np.mean((predictions - y_test) ** 2))
        # 将结果插入结果框
        model_name = model_name_list[i]
        results.loc[model_name, :] = [mae, rmse]
    # 中值基准度量
    baseline = np.median(y_train)
    baseline_mae = np.mean(abs(baseline - y_test))
    baseline_rmse = np.sqrt(np.mean((baseline - y_test) ** 2))
    results.loc['Baseline', :] = [baseline_mae, baseline_rmse]
    return results


results = evaluate(X_train, X_test, y_train, y_test)
print(results)

# 找出最合适的模型
plt.figure(figsize=(12, 8))

# 平均绝对误差
ax = plt.subplot(1, 2, 1)
results.sort_values('mae', ascending=True).plot.bar(y='mae', color='b', ax=ax, fontsize=20)
plt.title('mean absolute error', fontsize=20)
plt.ylabel('MAE', fontsize=20)

# 均方根误差
ax = plt.subplot(1, 2, 2)
results.sort_values('rmse', ascending=True).plot.bar(y='rmse', color='r', ax=ax, fontsize=20)
plt.title('root mean square error', fontsize=20)
plt.ylabel('RMSE', fontsize=20)
plt.tight_layout()
plt.show()

# 保存线性回归模型
model = LinearRegression()
model.fit(X_train, y_train)
filename = 'LR_Model'
pickle.dump(model, open(filename, 'wb'))

