import os
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

script_dir = os.path.dirname(__file__)
data_file_path = "../data/yob2019.txt"
data_file = os.path.join(script_dir, data_file_path)

sparkSession = SparkSession.builder.appName("FilterFemaleNames").getOrCreate()

schema = StructType([
    StructField('name', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('amount', IntegerType(), True)])

namesDF = sparkSession.read.schema(schema).csv(data_file)

femaleNamesDF = namesDF.filter(namesDF.gender == 'F')

femaleNamesDF.sort('name').show(truncate=False)

sparkSession.stop()
