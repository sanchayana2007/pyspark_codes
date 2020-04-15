#Run python3.7
from pyspark import SparkConf, SparkContext
import collections

print("Setting Spark context as master= Local machine not on cluster 1 thread : 1process ")
print("Setting Spark context as name is to identify the sc job")
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

#Please get the file in the context object 
#textfile loads and breaks up each line as  a value in RDD
lines = sc.textFile("/home/san/pyspark_codes/resourse/ml-100k/u.data")

#196(userid)	242(movieid)	3(Rating)	881250949(timestamp)
#Split by whitespace and Extract the Ratings for a Line and put all in a RDD(Ratings)
#Rating : [Row]=Rating   is mapped  
#Rating : 196(userid)    242(movieid)    3(Rating)       881250949(timestamp) : 3  
ratings = lines.map(lambda x: x.split()[2])
#Count all Value to get the Ratings occured
#results: {Value: Count of Value occured}
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
