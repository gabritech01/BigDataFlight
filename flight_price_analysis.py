from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, avg

spark = SparkSession.builder \
    .appName("FlightPriceAnalysisOptimized") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

gcs_path = "gs://practice1-436812_bucket/dataset.csv"

# uploading dataset
df = spark.read.csv(gcs_path, header=True, inferSchema=True).persist()

# Function to filter and calculate price
def analyze_prices(starting_airport=None, destination_airport=None, flight_date=None):
    filtered_df = df

    # filtering
    if starting_airport:
        filtered_df = filtered_df.filter(col("startingAirport") == starting_airport)
    if destination_airport:
        filtered_df = filtered_df.filter(col("destinationAirport") == destination_airport)
    if flight_date:
        filtered_df = filtered_df.filter(col("flightDate") == flight_date)
    
    # Min price, max price and average price
    result = filtered_df.select(
        min(col("totalFare")).alias("Min Price"),
        max(col("totalFare")).alias("Max Price"),
        avg(col("totalFare")).alias("Average Price")
    )
    
    # Time report
    import time
    start_time = time.time()
    result.show()
    print(f"Elapsed time: {time.time() - start_time:.2f} seconds")

# Esegui un esempio
analyze_prices(starting_airport="LGA", destination_airport="DTW", flight_date="2022-05-27")

# Chiudi la sessione Spark
spark.stop()