import sys
from pyspark import SparkContext

sc = SparkContext("local", "My App")



text_file = sc.textFile("file:///home/ma1306/creditspark/"+sys.argv[1])
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("file:///home/ma1306/creditspark/output")


