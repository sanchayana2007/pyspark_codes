import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("./resourse/book.txt")

#create a Regex with any Capital letter converted to small
words = input.flatMap(normalizeWords)

#For count of the word use --> (word,1)
#Groupby Key ie words and add 1s --> (Word,1+1)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)


#RDD dont have sortby Val so swap (count,word)--> (2,word)
#Sort on the Keys 
#** Although keys are same bu they are seperate RDDs  
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
