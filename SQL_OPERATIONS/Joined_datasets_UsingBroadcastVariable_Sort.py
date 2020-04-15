from pyspark import SparkConf, SparkContext
import collections

def loadMovieNames():
    movieNames = {}
    with open("/home/san/pyspark_codes/resourse/ml-100k/u.item",encoding = "ISO-8859-1") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames



conf = SparkConf().setMaster("local").setAppName("JoinDatasets")
sc = SparkContext(conf = conf)

#Please get the file in the context object 
#textfile loads and breaks up each line as  a value in RDD

lines = sc.textFile("/home/san/pyspark_codes/resourse/ml-100k/u.data")

#Row :  #196(userid)    242(movieid)    3(Rating)   881250949(timestamp) make tuple form dict--> {242(mov id),1}
movieid_rdd= lines.map(lambda x:(x.split()[1],1))

#(242,1) Groupby keys and add the Values--> {242,1+1....} --> (242(mov id),117(Total count))
Countofmovieviews = movieid_rdd.reduceByKey(lambda x, y: x + y)


#Flip key Value {242,117} --> {117(Total count),242(mov id)}
#Sortby key (Total Count)--> ....(117,242)
sortedResults = Countofmovieviews.map(lambda x: (x[1], x[0])).sortByKey()

#Create a broadcast variable 
#Broadcast varaibles keeps the data in Central place and all the excuters can use it at the time of calculation 
#if the tablesize i big broadcast a good practice to make things ok
nameDict = sc.broadcast(loadMovieNames())
#print(nameDict.value[50])

sortedMoviesWithNames = sortedResults.map(lambda x : (nameDict.value[int(x[1])],x[0]))

#collect and print Results one at a Time 
'''
results = sortedResults.collect()
for value in results:
    if int(value[1]) in nameDict.value:
        print(value[0],nameDict.value[int(value[1])])
    else:
        print("Val not found",value[1])

'''

results = sortedMoviesWithNames.collect()
for value in results:
    print("No of Views",value[1],"Movie:",value[0])

