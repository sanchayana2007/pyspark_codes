from pyspark import SparkConf, SparkContext
import collections

def parseLine(line):

    #Row :  #196(userid)    242(movieid)    3(Rating)   881250949(timestamp)
    #return {movieid:row}-> (44,23.44)
    fields = line.split()
    
     
    return (fields[1],1)




conf = SparkConf().setMaster("local").setAppName("MostOccuringValue")
sc = SparkContext(conf = conf)

#Please get the file in the context object 
#textfile loads and breaks up each line as  a value in RDD
lines = sc.textFile("/home/san/pyspark_codes/resourse/ml-100k/u.data")

movieid_rdd= lines.map(parseLine)
Countofmovieviews = movieid_rdd.reduceByKey(lambda x, y: x + y)
sortedResults = Countofmovieviews.map(lambda x: (x[1], x[0])).sortByKey()
results = sortedResults.collect()
for value in results:
        print(value[0], value[1])



