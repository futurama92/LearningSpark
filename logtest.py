from pyspark import SparkConf, SparkContext

conf = SparkContext.setMaster("local").setAppName("log")
sc = SparkContext(conf = conf)

lines = sc.textFile("log.txt")
errorRDD = lines.filter(lambda x: "error" in x)
warningRDD = lines.filter(lambda x: "warning" in x)

print "Errori: " + str(errorRDD.count())
print "Warning: " + str(warningRDD.count())
