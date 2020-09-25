import os
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window

script_dir = os.path.dirname(__file__)
data_file_path = "../data/yob2019.txt"
data_file = os.path.join(script_dir, data_file_path)

sparkSession = SparkSession.builder.appName("RankedFemaleNames").getOrCreate()

schema = StructType([
    StructField('name', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('amount', IntegerType(), True)])

namesDF = sparkSession.read.schema(schema).csv(data_file)

femaleNamesDF = namesDF.filter(namesDF.gender == 'F')
nameSpec = Window.orderBy(functions.desc("amount"))

results = femaleNamesDF.withColumn(
    "rank", functions.dense_rank().over(nameSpec))

results.coalesce(1).write.format("csv").mode('overwrite').save(
    "results/female_rank.csv", header="true")

sparkSession.stop()
