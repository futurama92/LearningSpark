from pyspark import SparkConf, SparkContext

# Inizialize sparkContext
conf = SparkConf().setMaster("local").setAppName("logtest")
sc = SparkContext(conf = conf)

# Operetion on file
lines = sc.textFile("log.txt")
errorRDD = lines.filter(lambda x: "error" in x)
warningRDD = lines.filter(lambda x: "warning" in x)

# Output
print "Errori: " + str(errorRDD.count())
print "Warning: " + str(warningRDD.count())
