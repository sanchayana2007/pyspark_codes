from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("./resourse/1800.csv")

#Data : ITE00100554,18000101,TMAX,-75,,,E,
#Data:  ITE00100554,18000101,TMIN,-148,,,Ei
# Any line dont have teh temp field is automatically Filterd out 
#parseline--> (ITE00100554,TMAX,-75)
parsedLines = lines.map(parseLine)

#Filter and keep--> (ITE00100554,TMIN,-75)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])

#keep--> (ITE00100554,-75)
stationTemps = minTemps.map(lambda x: (x[0], x[2]))

'''
results = stationTemps.collect()
for result in results:
        print("stationTemps",result)
'''

#Reduce the RDD by key ie group by Stationid and get the min= SQL Group stn id  by and get min Temp--> (ITE00100554  5.36F,EZE00100082  7.70F)
#** All the Non Exitant fields are already Filtered before
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))

results = minTemps.collect();
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
