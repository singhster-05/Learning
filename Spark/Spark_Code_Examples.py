#Pyspark tutorial

from operator import add

sc = SparkContext()

text = sc.textFile("/Documents/GitHub/Introduction\ to\ PySpark\ -\ Working\ Files/Chapter\ 3/Audio\ Standardization\ Sentences.txt")

words = ip_stream.flatMap(lambda x : x.split(' ')).map(lambda x : (x,1)).reduceByKey(add)

output = words.collect()

for count, word in output:
    print("%i %s" % (count,word))

ip = sc.parallelize(xrange(10000))

ip.map(lambda x : x**5).collect()

import numpy as np
import matplotlib as mlp
import matplotlib.pyplot as plt

mlp.style.use('ggplot')
x = np.linspace(0,20,500)
plt.plot(x,np.sin(x))

ip = sc.parallelize(xrange(100), numSlices = 5)
ip.glom().collect()     #Glom allows to operate over different arrays as a single array

ip.takeSample(withReplacement=True, num=100, seed = 13579)

words = text.flatMap(lambda x : x.split(' '))

def count_words(iterator):
    counts = {}
    for w in iterator:
        if w in counts:
            counts[w] += 1
        else:
            counts[w] = 1
    yield counts

word_count = words.mapPartitions(count_words).collect()

pairs = sc.parallelize([("a",1),("b",2),("b",4),("c",34)])

ip.filter(lambda x : (x%2==0)).collect()

def add_to_queue(x,queue=[]):
    queue+= [x]
    return queue

add_to_queue(5)
add_to_queue(6)

ip.foreach(add_to_queue)

[x for x in pairs.groupByKey().collec()[0][1]]

states = sc.parallelize(["TX","TX","CA","MN","MN","CA","TX"])

states.map(lambda x : (x,1)).reduceByKey(add).collect()

zero_value = set()
def seq_op(x,y):
    x.add(y)
    return x

def comb_op(x,y):
    return x.union(y)

ip = sc.parallelize([0,1,2,3,4,6,7,8,3,1,12,4,55,6,32,31,3]).map(lambda x :['even' if x%2 == 0 else 'odd',x])
ip.aggregateByKey(zero_value,seq_op,comb_op)

ip.aggregateByKey(0,lambda acc, val:acc+val, lambda acc1, acc2:acc1+acc2).collect()

#AverageByKey using aggregateByKey
aTuple = (0,0)
op = ip.aggregateByKey(aTuple, lambda a,b: (a[0] + b, a[1] + 1),lambda a,b: (a[0] + b[0], a[1] + b[1]))
output = sc.parallelize(op)
output.mapValues(lambda x : x[0]/x[1]).collect()


#AverageByKey using combineByKey
data = [("A", 2.), ("A", 4.), ("A", 9.),
        ("B", 10.), ("B", 20.),
        ("Z", 3.), ("Z", 5.), ("Z", 8.), ("Z", 12.)]

ip = sc.parallelize(data)

sumCount = ip.combineByKey(lambda x:(x,1),
                            lambda x, value: (x[0]+value, x[1]+1),
                            lambda x,y: (x[0]+y[0], x[1]+y[1]))
averageByKey = sumCount.map(lambda (key, (totSum,count)):(key, totSum/count)).collectAsMap()

#Sorting
ip.sortByKey(numPartitions=3).glom().collect()

ip.sortByKey(keyfunc=lambda x:x.lower()).collect()

#Output join with single key Output
ip.cogroup(op).mapValues(lambda x: [list(x[0]),list(x[1])]).collect()

#Pipe Operator
ip = sc.parallelize(xrange(100))

ip.pipe("grep 1").collect()

#Spark Master URL spark://dca9046e8ae1:7077     http://10.108.226.37:8080

#Start master node spark ./sbin/start-master.sh     slave node ./sbin/start-slave.sh
#Stop master node ./sbin/stop-master.sh

from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc,1)

#wordCount from stream local directory

ip_stream = ssc.textFileStream('/Desktop/sampleText')
words = ip_stream.flatMap(lambda x : x.split(' ')).map(lambda x : (x,1)).reduceByKey(add)
words.pprint()
ssc.start()
ssc.stop()

#SparkSQL

from pyspark.sql import SQLContext
sqlC = SQLContext(sc)
s = sqlC.read.json("/Documents/GitHub/Introduction\ to\ PySpark\ -\ Working\ Files/Chapter\ 10/sample.json")
s.printSchema()  #Show table schema
s.show()         #Show Table

#Spark SQL API
s.filter(s['price'] > 12).select(s['name']).show()

#Spark SQL Commands
s.registerTempTable('sample')
sqlC.sql('select name from sample where price >= 12').collect()
