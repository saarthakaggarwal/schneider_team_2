from flask import Flask, request, render_template, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
import time
import logging
import pandas as pd

# Initialize Flask application
app = Flask(__name__)
logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Create Spark session with optimal memory settings and explicit garbage collection settings
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("FreightSearchApp") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.memory.fraction", "0.9") \
    .getOrCreate()

# Define paths to CSV data files
load_posting_path = '/Users/tanmaymodi/Schneider/load_posting.csv'
load_stop_path = '/Users/tanmaymodi/Schneider/load_stop.csv'

# Define schemas for CSV files to improve loading performance and avoid schema inference issues
postingSchema = StructType([
    StructField("LOAD_ID", IntegerType(), True),
    StructField("POSTING_STATUS", StringType(), True),
    StructField("TRANSPORT_MODE", StringType(), True),
    StructField("IS_HAZARDOUS", BooleanType(), True),
    StructField("IS_HIGH_VALUE", BooleanType(), True)
])

stopSchema = StructType([
    StructField("LOAD_ID", IntegerType(), True),
    StructField("CITY", StringType(), True),
    StructField("STATE", StringType(), True),
    StructField("APPOINTMENT_FROM", StringType(), True),
    StructField("APPOINTMENT_TO", StringType(), True)
])

# Function to load data with defined schema
def load_data():
    load_posting = spark.read.csv(load_posting_path, header=True, schema=postingSchema).cache()
    load_stop = spark.read.csv(load_stop_path, header=True, schema=stopSchema).cache()
    return load_posting, load_stop

# Load and cache data upon startup to improve performance
load_posting, load_stop = load_data()

# Function to perform search based on user-provided parameters
def perform_search(params):
    start_time = time.time()
    merged_data = load_posting.join(broadcast(load_stop), "LOAD_ID", "inner")

    # Dynamically building filter conditions based on input parameters
    conditions = [
        col('TRANSPORT_MODE').contains(params['trailer_type']) if params.get('trailer_type') else None,
        col('IS_HAZARDOUS') == (params['is_hazardous'] == 'on') if 'is_hazardous' in params else None,
        col('IS_HIGH_VALUE') == (params['is_high_value'] == 'on') if 'is_high_value' in params else None,
        col('CITY').contains(params['origin_city']) if params.get('origin_city') else None,
        col('STATE').contains(params['origin_state']) if params.get('origin_state') else None,
        col('APPOINTMENT_FROM').contains(params['pickup_date']) if params.get('pickup_date') else None,
        col('APPOINTMENT_TO').contains(params['delivery_date']) if params.get('delivery_date') else None
    ]
    conditions = [c for c in conditions if c is not None]

    # Apply all conditions to the data
    if conditions:
        for condition in conditions:
            merged_data = merged_data.filter(condition)

    # Convert Spark DataFrame to Pandas for rendering in Flask, handling empty results gracefully
    try:
        results_df = merged_data.toPandas() if not merged_data.rdd.isEmpty() else pd.DataFrame()
        search_time = time.time() - start_time
        logging.info(f"Search completed in {search_time} seconds, final result count: {results_df.shape[0]}")
    except Exception as e:
        logging.error(f"Error during DataFrame conversion or processing: {e}")
        results_df = pd.DataFrame()
        search_time = time.time() - start_time
    return results_df, search_time

# Flask route to handle search requests
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        search_params = request.form.to_dict()
        results, search_time = perform_search(search_params)
        results_html = results.to_html(index=False) if not results.empty else "No results found."
        return render_template('index.html', results=results_html, search_time=f"{search_time:.2f} seconds")
    return render_template('index.html')

# Main entry point for Flask application
if __name__ == '__main__':
    app.run(debug=True)
