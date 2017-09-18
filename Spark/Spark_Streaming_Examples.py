from operator import add, sub
from time import sleep
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Set up the Spark context and the streaming context
sc = SparkContext(appName="PysparkNotebook")
ssc = StreamingContext(sc, 1)

# Input data
rddQueue = []
for i in range(5):
    rddQueue += [ssc.sparkContext.parallelize([i, i+1])]

inputStream = ssc.queueStream(rddQueue)

inputStream.map(lambda x: "Input: " + str(x)).pprint()
inputStream.reduce(add)\
    .map(lambda x: "Output: " + str(x))\
    .pprint()

ssc.start()
sleep(5)

#Add the input data over a stream

sc = SparkContext(appName="PysparkNotebook")
ssc = StreamingContext(sc, 1)

inputData = [
    [1,2,3],
    [0],
    [4,4,4],
    [0,0,0,25],
    [1,-1,10],
]

rddQueue = []
for datum in inputData:
    rddQueue += [ssc.sparkContext.parallelize(datum)]

inputStream = ssc.queueStream(rddQueue)
inputStream.reduce(add).pprint()

ssc.start()
sleep(5)
ssc.stop(stopSparkContext=True, stopGraceFully=True)

#update the rdd every 1 second for the last 5 seconds

sc = SparkContext(appName="ActiveUsers")
ssc = StreamingContext(sc, 1)

activeUsers = [
    ["Alice", "Bob"],
    ["Bob"],
    ["Carlos", "Dan"],
    ["Carlos", "Dan", "Erin"],
    ["Carlos", "Frank"],
]

rddQueue = []
for datum in activeUsers:
    rddQueue += [ssc.sparkContext.parallelize(datum)]

inputStream = ssc.queueStream(rddQueue)
inputStream.window(5, 1)\
    .map(lambda x: set([x]))\
    .reduce(lambda x, y: x.union(y))\
    .pprint()

ssc.start()
sleep(5)
ssc.stop(stopSparkContext=True, stopGraceFully=True)

#Streaming Additions

sc = SparkContext(appName="HighScores")
ssc = StreamingContext(sc, 1)
ssc.checkpoint("highscore-checkpoints")

player_score_pairs = [
    [("Alice", 100), ("Bob", 60)],
    [("Bob", 60)],
    [("Carlos", 90), ("Dan", 40)],
    [("Carlos", 10), ("Dan", 20), ("Erin", 90)],
    [("Carlos", 20), ("Frank", 200)],
]

rddQueue = []
for datum in player_score_pairs:
    rddQueue += [ssc.sparkContext.parallelize(datum)]

inputStream = ssc.queueStream(rddQueue)
inputStream.reduceByKeyAndWindow(add, sub, 3, 1)\
    .pprint()

ssc.start()
sleep(5)
ssc.stop(stopSparkContext=True, stopGraceFully=True)

#Streaming ML

#Create train & test from data
from pyspark.mllib.regression import LabeledPoint
import random

random.seed(1111)

test_data = []
train_data = []
for i in range(10):
    train_data += [[]]

with open("temperature_data_small.txt") as t:
    for l in t:
        fields = l.split()
        point = LabeledPoint(fields[22],
                             [fields[7], fields[8], fields[11], fields[21]])
        if random.random() >= 0.8:
            test_data += [point]
        else:
            train_data[random.randrange(10)] += [point]

#Create train and test data streams
from pyspark.mllib.regression import StreamingLinearRegressionWithSGD

sc = SparkContext(appName="HumidityPrediction")
ssc = StreamingContext(sc, 2)

training_data_stream = ssc.queueStream(
    [ssc.sparkContext.parallelize(d) for d in train_data])
test_data_stream = ssc.queueStream(
    [test_data for d in train_data])\
    .map(lambda lp: (lp.label, lp.features))

#Initialize model with weights & setup model
model = StreamingLinearRegressionWithSGD(
    numIterations=5,
    stepSize=0.00005)
model.setInitialWeights([1.0,1.0,1.0,1.0])

model.trainOn(training_data_stream)

#Make predictions and create custom outputs
predictions = model.predictOnValues(test_data_stream)\
    .map(lambda x: (x[0], x[1], (x[0] - x[1])))

predictions.map(lambda x:
                "Actual: " + str(x[0])
                + ", Predicted: " + str(x[1])
                + ", Error: " + str(x[2])).pprint()
predictions.map(lambda x: (x[2]**2, 1))\
    .reduce(lambda x,y: (x[0] + y[0], x[1] + y[1]))\
    .map(lambda x: "MSE: " + str(x[0]/x[1]))\
    .pprint()

#Start the model stream
ssc.start()
for i in range(10):
    sleep(2)
    print(model.latestModel())
ssc.stop(stopSparkContext=True, stopGraceFully=True)
