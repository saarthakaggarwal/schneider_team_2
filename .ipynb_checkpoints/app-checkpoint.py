from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
import time


spark = SparkSession.builder.appName("LoadSearchAlgorithm").getOrCreate()

posting_data = spark.read.csv("./load_posting.csv", header=True, inferSchema=True)
stop_data = spark.read.csv("./load_stop.csv", header=True, inferSchema=True)


def testing(type_truck, radius, weight, posting_data, stop_data, origin, dest):
    
    start_time = time.time()
    
    filtered_posting_data = posting_data.filter(
        (col("TRANSPORT_MODE") == type_truck) &
        (col("TOTAL_WEIGHT").cast("float") <= weight) &
        (col("TOTAL_DISTANCE").cast("float") <= radius)
    )
    
    filtered_ids = filtered_posting_data.select("LOAD_ID")
    joined_data = filtered_ids.join(stop_data, "LOAD_ID", "inner")
    
    # Step 4: Identify matching origins and destinations
    matching_stops = joined_data.withColumn(
        "is_origin", when((col("STOP_SEQUENCE") == "1") & (col("STATE") == origin), True).otherwise(False)
    )
    
    
    matching_origins = matching_stops.groupBy("LOAD_ID").agg(
        {"is_origin": "max"}
    ).filter(col("max(is_origin)") == True)
    
    matching_origins.show()
    
    matching_ids = [row["LOAD_ID"] for row in matching_origins.collect()]
    print(matching_ids)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print(f"Test Case 1 Result:")
    print(f"Time taken: {elapsed_time:.6f} seconds\n")
    return matching_ids