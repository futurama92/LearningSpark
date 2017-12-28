from pyspark import SparkConf, SparkContext

# Inizialize sparkContext
conf = SparkConf().setMaster("local").setAppName("logtest")
sc = SparkContext(conf = conf)

# Operetion on file
lines = sc.textFile("log.txt")
errorRDD = lines.filter(lambda x: "error" in x)
warningRDD = lines.filter(lambda x: "warning" in x)
badRDD = errorRDD.union(warningRDD)

# Output
print "Errori: " + str(errorRDD.count())
print "Warning: " + str(warningRDD.count())
print "Union: 1" + str(badRDD.count())

for line in badRDD.take(10):
    print line
