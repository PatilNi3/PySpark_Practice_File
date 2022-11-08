# What is PySpark ?
'''
- PySpark is the Python API for Apache Spark, an open source, distributed computing framework and set of libraries for
  real-time, large-scale data processing.
- PySpark provides "Py4j" library, with the help of this library, Python can be easily integrated with Apache Spark.
- PySpark plays an essential role when it needs to work with a vast dataset or analyze them.
'''
# What is Apache Spark ?
'''
- Apache Spark is an open-source distributed cluster-computing framework introduced by Apache Software Foundation. 
- It is a general engine for big data analysis, processing and computation. 
- It is built for high speed, ease of use, offers simplicity, stream analysis and run virtually anywhere. 
- It can analyze data in real-time. It provides fast computation over the big data.
- It can be used for multiple things like running distributed SQL, creating data pipelines, ingesting data into a 
  database, running Machine Learning algorithms, working with graphs or data streams, and many more.
'''
# Why PySpark ?
'''
A large amount of data is generated offline and online. These data contain the hidden patterns, unknown correction, 
market trends, customer preference and other useful business information. It is necessary to extract valuable information 
from the raw data.
'''

# pip install pyspark
import pyspark

# pip install pandas
import pandas as pd

# READING DATASET USING PANDAS
'''
data = pd.read_csv("MiniDatasetForPySpark.csv")

df = pd.DataFrame(data)
print(df)
'''

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("pyspark1").getOrCreate()

# print(spark)

# READING DATASET USING PYSPARK
'''
data = spark.read.option('header', 'true').csv("MiniDatasetForPySpark.csv")
print(data)

data = spark.read.option('header', 'true').csv("NitinData.csv")
'''
data = spark.read.csv("Nitin.csv", header=True, inferSchema=True)


# data.show()

# CHECKING SCHEMA OF DATASET (in very simple words schema = datatypes)
'''
schema = data.printSchema()
'''

# CHECKING DATATYPE OF SINGLE OR MULTIPLE COLUMN
'''
print(data.select("Name", "Age"))
'''

# DATATYPES OF ALL COLUMN
'''
print(data.dtypes)
'''

# TYPE OF DATAFRAME
'''
print(type(data))
'''

# TO SEE TOP AND BOTTOM ROWS
'''
print(data.head())

print(data.head(3))

print(data.tail(1))

print(data.tail(3))
'''

# FETCHING ALL COLUMNS OF DATASETS
'''
print(data.columns)
'''

# DESCRIBE DATASET
'''
print(data.describe().show())
'''

# ADDING COLUMN TO THE DATAFRAME
'''
New_Column = data.withColumn("Age after 5 year", data['Age']+5)
New_Column.show()

OR

data.withColumn("Age after 5 year", data['Age']+5).show()
'''

# DROPPING COLUMNS FROM DATAFRAME
'''
data.drop("Age after 5 year").show()
'''

# RENAME COLUMNS
'''
data.withColumnRenamed("Salary", "Package").show()
'''

# HANDLING NULL VALUES

# 1. DROPPING ROWS HAVING NULL VALUES
'''
data.na.drop().show()

data.na.drop(how='any').show()          # atleast 1 Null value should be in the row

Drop_Name = data.drop("Name").show()
Drop_Name.na.drop(how='all').show()          # all column values should be Null for that row only

data.na.drop(how='any', thresh=2).show()     # atleast 2 non-null values should present in row

data.na.drop(how='any', thresh=3).show()     # atleast 3 non-null values should present in row

data.na.drop(how='any', subset=['Age']).show()  # any null values in Age column, that row will be dropped
'''

# 2. FILLING NULL VALUES
'''
data.na.fill(55555).show()

data.na.fill(25, 'Salary').show()

data.na.fill(0, ['Salary','Experience']).show()
'''

# FILTER OPERATIONS: & , | , ==

# QUE.: SALARY SHOULD BE GREATER THAN OR EQUAL TO 1000000
'''
data.filter("Salary >= 1000000").show()
or
data.filter(data["Salary"] >= 1000000).show()
'''

# AND (&) Logic
'''
data.filter((data["Salary"] >= 1000000) & (data["Age"] >= 27)).show()

data.filter((data["Salary"] < 100000) & (data["Experience"] > 5)).show()
'''

# OR (|) Logic
'''
data.filter((data["Salary"] >= 1000000) | (data["Age"] >= 27)).show()

data.filter((data["Salary"] < 100000) | (data["Experience"] > 5)).show()
'''

# NOT or INVERSE FILTER (~)
'''
data.filter(~(data["Salary"] < 1200000)).show()

data.filter(~(data["Salary"] >= 1000000) & (~(data["Age"] >= 27))).show()
'''

# ☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻

from pyspark.ml.feature import Imputer
'''
Impute = Imputer(inputCols=["Age", "Experience"], outputCols=["{}_Imputed".format(c) for c in ["Age", "Experience"]]).setStrategy("mean")

Impute.fit(data).transform(data).show()
'''

# GROUP BY OPERATION
'''
data1 = spark.read.csv("Dataset.csv", header=True, inferSchema=True)
'''
# SUM
'''
data1.groupBy("Department").sum().show()

data1.groupby("Package").sum().show()

data1.groupby("Age").sum().show()

data1.agg({"Age" : "sum"}).show()           # overall

data1.agg({"Package" : "sum"}).show()       # overall
'''

# MEAN
'''
data1.groupBy("Department").mean().show()

data1.groupby("Package").mean().show()

data1.groupby("Age").mean().show()

data1.agg({"Age" : "mean"}).show()

data1.agg({"Package" : "mean"}).show()
'''

# COUNT
'''
data1.groupBy("Department").count().show()

data1.groupby("Package").count().show()

data1.groupby("Age").count().show()

data1.groupBy({"Package" : "count"}).show()             # Error
'''

# MAX
'''
data1.groupBy("Department").max().show()

data1.groupby("Package").max().show()

data1.groupby("Age").max().show()

data1.agg({"Package" : "max"}).show()

data1.agg({"Age" : "max"}).show()
'''

# MIN
'''
data1.groupby("Department").min().show()

data1.groupby("Package").min().show()

data1.groupby("Age").min().show()

data1.agg({"Department" : "min"}).show()

data1.agg({"Package" : "min"}).show()

data1.agg({"Age" : "min"}).show()
'''

# ☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻

# LINEAR REGRESSION PROBLEM
'''
data2 = spark.read.csv("Dataset2.csv", header=True, inferSchema=True)
'''
'''
print(data2.columns)

data2.printSchema()
'''

from pyspark.ml.feature import VectorAssembler
'''
Feature_Assembler = VectorAssembler(inputCols=["Age", "Experience"], outputCol="Independent_Feature")

Output = Feature_Assembler.transform(data2)
'''
# Output.show()

# print(Output.columns)
'''
Finalised_Data = Output.select("Independent_Feature", "Package")
Finalised_Data.show()
'''

# TRAIN TEST SPLIT

from pyspark.ml.regression import LinearRegression
'''
Train_Data, Test_Data = Finalised_Data.randomSplit([0.75, 0.25])

Regressor = LinearRegression(featuresCol="Independent_Feature", labelCol="Package")

Regressor_Fit = Regressor.fit(Train_Data)
'''

# COEFFICIENT
'''
print(Regressor_Fit.coefficients)               # [77348.83095942085,-9348.293469498221]
'''

# INTERCEPT
'''
print(Regressor_Fit.intercept)                  # -2774481.017392832
'''

# PREDICTION
'''
Pred_Result = Regressor_Fit.evaluate(Test_Data)

Pred_Result.predictions.show()
'''

# ☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻☺☻

