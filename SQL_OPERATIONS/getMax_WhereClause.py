from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf = conf)

def countCoOccurences(line):
     #5983(Heroid) 1165(frnd1) 3836(frnd2) 4361 1282 716 4289 4646 6300 5084 2397 4454 1913 5861 5485(frndn )
     # remaning self count  ---> (5983,17-1)-->(5983,16)
     elements = line.split()
     return (int(elements[0]), len(elements) - 1)
    
def parseNames(line):
     fields = line.split()
    #3(heroid) "4-D MAN/MERCURIO"(name)-->(3,"4-D MAN/MERCURIO")
     return (int(fields[0]), fields[1].encode("utf8"))

names = sc.textFile("/home/san/pyspark_codes/resourse/Marvel_heros_name_id.txt")
namesRdd = names.map(parseNames)

lines = sc.textFile("/home/san/pyspark_codes/resourse/Coenction_Graph_marvel_heroes.txt")
pairings = lines.map(countCoOccurences)

#Get All the friends count for each hero (5983,16)--> (5983,100)
totalFriendsByCharacter = pairings.reduceByKey(lambda x, y : x + y)

#--> (100,5983)
flipped = totalFriendsByCharacter.map(lambda x : (x[1], x[0]))

#get teh max frinds in the RDD-->100
mostPopular = flipped.max()

#selct Charname from tableB where  maxid = 5983 -- > CAPTAIN
mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print(str(mostPopularName) + " is the most popular superhero, with " + \
            str(mostPopular[0]) + " co-appearances.")

