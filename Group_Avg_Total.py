from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):

    #Row :   3(userid),Deanna(Name),40(Age),465(Noof Friends)
    #return {Age:NoFriends}-> (40,465)
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("./resourse/fakefriends.csv")

#Create a key Value RDD dict of Age , Num of friends--> {40,465}
rdd = lines.map(parseLine)

#mapValues(lambda x: (x, 1)) -->  (Values ie No of friends,1)--> {40: (465,1)}
friends1tuple = rdd.mapValues(lambda x: (x, 1))

#reduceByKey= Aggregate on Key ie Age sum (NoFrinds,Count of Age)-->  {40: (465+243,1+1)}-->  {40: (708,2)}
#In reduceByKey it will groupby key and  all operations will be o the Values of subsequent 
totalsByAge =friends1tuple.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
print("totals by Age ")
results = totalsByAge.collect()
for result in results:
    print(result)
#Map on Value by Averages {40: (708,2)}--> {40: (708/2}
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

print("Average by Age ")
#Create Tuples of Dict and Make a List o display
results = averagesByAge.collect()
for result in results:
    print(result)
