""" Reading a simple file"""
from pyspark.sql import SparkSession

logFile = '/home/kuzman/Desktop/agreeNote.txt'
spark = SparkSession.builder\
    .appName("StructuredNetworkWordCount")\
    .getOrCreate()

logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()
