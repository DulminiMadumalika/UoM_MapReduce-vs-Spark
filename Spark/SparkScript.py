from pyspark.sql import SQLContext, SparkSession
import time

session = SparkSession.builder.appName("SparkAssignment").getOrCreate()
sqlContext = SQLContext(session)
dataFrameReader = sqlContext.read
delay_flights = dataFrameReader.option("header", "true").option("inferSchema", value=True)\
    .csv("s3://wpdbucket/dataset/DelayedFlights-updated.csv")

print("=== Print out schema ===")
delay_flights.printSchema()
delay_flights.registerTempTable("delay_flights")

# Query for Carrier Delay --------------------------------------------------------------------------------
for i in range(5):
    print("=== Start Query ===", i + 1)
    start_time = time.time()
    result = sqlContext.sql("SELECT Year, avg((CarrierDelay /ArrDelay)*100) from delay_flights GROUP BY Year")
    result.show()
    end_time = time.time()
    print("=== End Query ===", i + 1)
    elapsed_time = end_time - start_time
    print("Time spent for Query : ", i+1, " : ", elapsed_time)

# Query for Weather Delay --------------------------------------------------------------------------------
for i in range(5):
    print("=== Query 2 Started ===", i+1)
    start_time1_q2 = time.time()
    result = sqlContext.sql("SELECT Year, avg((WeatherDelay /ArrDelay)*100) from delay_flights GROUP BY Year")
    result.show()
    end_time1_q2 = time.time()
    print("=== Query 2 Ended ===", i+1)
    elapsed_time2 = end_time1_q2 - start_time1_q2
    print("Elapsed time for query2 : ", i+1, elapsed_time2)

# Query for NAS Delay --------------------------------------------------------------------------------
for i in range(5):
    print("=== Query 3 Started ===", i+1)
    start_time1_q3 = time.time()
    result = sqlContext.sql("SELECT Year, avg((NASDelay /ArrDelay)*100) from delay_flights GROUP BY Year")
    result.show()
    end_time1_q3 = time.time()
    print("=== Query 3 Ended ===", i+1)
    elapsed_time3 = end_time1_q3 - start_time1_q3
    print("Elapsed time for query3 : ", elapsed_time3)

# Query for Security Delay --------------------------------------------------------------------------------
for i in range(5):
    print("=== Query 4 Started ===", i+1)
    start_time1_q4 = time.time()
    result = sqlContext.sql("SELECT Year, avg((SecurityDelay /ArrDelay)*100) from delay_flights GROUP BY Year")
    result.show()
    end_time1_q4 = time.time()
    print("=== Query 4 Ended ===", i+1)
    elapsed_time4 = end_time1_q4 - start_time1_q4
    print("Elapsed time for query4 : ", elapsed_time4)

# Query for LateAirCraft Delay --------------------------------------------------------------------------------
for i in range(5):
    print("=== Query 5 Started ===", i+1)
    start_time1_q5 = time.time()
    result = sqlContext.sql("SELECT Year, avg((LateAirCraftDelay /ArrDelay)*100) from delay_flights GROUP BY Year")
    result.show()
    end_time1_q5 = time.time()
    print("=== Query 5 Ended ===", i+1)
    elapsed_time5 = end_time1_q5 - start_time1_q5
    print("Elapsed time for query5 : ", elapsed_time5)



