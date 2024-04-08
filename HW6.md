### Question 1: Redpanda version
What's the version, based on the output of the command you executed? (copy the entire version)
**v22.3.5 (rev 28b2443)**

### Question 2. Creating a topic
What's the output of the command for creating a topic?
```
TOPIC       STATUS
test-topic  OK
```

### Question 3. Connecting to the Kafka server
```
True

Process finished with exit code 0
```

### Question 4. Sending data to the stream
How much time did it take? Where did it spend most of the time?
```
took 0.51 seconds
Sending the messages
```

### Question 5: Sending the Trip Data
Create a topic green-trips and send the data there
How much time in seconds did it take? (You can round it to a whole number)
Make sure you don't include sleeps in your code
Creating the PySpark consumer
Now let's read the data with PySpark.

Spark needs a library (jar) to be able to connect to Kafka, so we need to tell PySpark that it needs to use it:

import pyspark
from pyspark.sql import SparkSession

pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("GreenTripsConsumer") \
    .config("spark.jars.packages", kafka_jar_package) \
    .getOrCreate()
Now we can connect to the stream:

green_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "green-trips") \
    .option("startingOffsets", "earliest") \
    .load()
In order to test that we can consume from the stream, let's see what will be the first record there.

In Spark streaming, the stream is represented as a sequence of small batches, each batch being a small RDD (or a small dataframe).

So we can execute a function over each mini-batch. Let's run take(1) there to see what do we have in the stream:

def peek(mini_batch, batch_id):
    first_row = mini_batch.take(1)

    if first_row:
        print(first_row[0])

query = green_stream.writeStream.foreachBatch(peek).start()
You should see a record like this:

Row(key=None, value=bytearray(b'{"lpep_pickup_datetime": "2019-10-01 00:26:02", "lpep_dropoff_datetime": "2019-10-01 00:39:58", "PULocationID": 112, "DOLocationID": 196, "passenger_count": 1.0, "trip_distance": 5.88, "tip_amount": 0.0}'), topic='green-trips', partition=0, offset=0, timestamp=datetime.datetime(2024, 3, 12, 22, 42, 9, 411000), timestampType=0)
Now let's stop the query, so it doesn't keep consuming messages from the stream

query.stop()
Question 6. Parsing the data
The data is JSON, but currently it's in binary format. We need to parse it and turn it into a streaming dataframe with proper columns

Similarly to PySpark, we define the schema

from pyspark.sql import types

schema = types.StructType() \
    .add("lpep_pickup_datetime", types.StringType()) \
    .add("lpep_dropoff_datetime", types.StringType()) \
    .add("PULocationID", types.IntegerType()) \
    .add("DOLocationID", types.IntegerType()) \
    .add("passenger_count", types.DoubleType()) \
    .add("trip_distance", types.DoubleType()) \
    .add("tip_amount", types.DoubleType())
And apply this schema:

from pyspark.sql import functions as F

green_stream = green_stream \
  .select(F.from_json(F.col("value").cast('STRING'), schema).alias("data")) \
  .select("data.*")
How does the record look after parsing? Copy the output

### Question 7: Most popular destination
Now let's finally do some streaming analytics. We will see what's the most popular destination currently based on our stream of data (which ideally we should have sent with delays like we did in workshop 2)

This is how you can do it:

Add a column "timestamp" using the current_timestamp function
Group by:
5 minutes window based on the timestamp column (F.window(col("timestamp"), "5 minutes"))
"DOLocationID"
Order by count
You can print the output to the console using this code

query = popular_destinations \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
Write the most popular destanation. (You will need to re-send the data for this to work)

Submitting the solutions
Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw6
Solution
We will publish the solution here after deadline.
