from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerTotal")
sc = SparkContext(conf = conf)

def parseLine(line):

    #Row :   44(userid),Deanna(Name),3233(Prodcode),23.44(Price)
    #return {Age:NoFriends}-> (44,23.44)
    fields = line.split(',')
    Custid = fields[0]
    Price = float(fields[2])# Its mandatory to add 
    return (Custid, Price)

lines = sc.textFile("/home/san/pyspark_codes/resourse/customer-orders.csv")

#Create a key Value RDD dict of Cid , Price of items--> {44:23.44}
cust_orders = lines.map(parseLine)

#Groupby Custid and Add the Prices--> '44', 4756.88
sumofpurchase = cust_orders.reduceByKey(lambda x, y: x + y)
print("totals by Purchase ")
results = sumofpurchase.collect()
for result in results:
        print(result)

#Sort it 
#We use map ie  dict(k,v)=x
#swap k-v--->  (4756.88,'44')
sumofpurchaseSorted = sumofpurchase.map(lambda x: (x[1], x[0])).sortByKey()
results = sumofpurchaseSorted.collect()

for result in results:
    print (results)
