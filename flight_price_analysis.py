from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, avg
import time
from google.cloud import storage

spark = SparkSession.builder \
    .appName("FlightPriceAnalysisOptimized") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

gcs_path = "gs://practice1-436812_bucket/dataset.csv"
output_path = "gs://practice1-436812_bucket/output_project/flight_price_analysis_output.txt"

# Initialize GCS client
client = storage.Client()

# uploading dataset
df = spark.read.csv(gcs_path, header=True, inferSchema=True).dropDuplicates(["legId"]).persist()

# Function to filter and calculate price
def analyze_prices(starting_airport=None, destination_airport=None, flight_date=None):
    filtered_df = df
    # Time report
    start_time = time.time()
    # filtering based on user input
    if starting_airport:
        filtered_df = filtered_df.filter(col("startingAirport") == starting_airport)
    if destination_airport:
        filtered_df = filtered_df.filter(col("destinationAirport") == destination_airport)
    if flight_date:
        filtered_df = filtered_df.filter(col("flightDate") == flight_date)
    
    # Min price, max price, and average price
    result = filtered_df.select(
        min(col("totalFare")).alias("Min Price"),
        max(col("totalFare")).alias("Max Price"),
        avg(col("totalFare")).alias("Average Price")
    )
    
    
    result_data = result.collect()  

    result_str = "Analysis Results:\n"
    for row in result_data:
        result_str += f"Min Price: {row['Min Price']}, Max Price: {row['Max Price']}, Average Price: {row['Average Price']}\n"
        
    suggestions_str = ""
    if flight_date:
        suggestions_str += "\nTop 3 cheapest flights on the specified date:\n"
        flights_on_date = df.filter(col("flightDate") == flight_date) \
                            .select("legId", "totalFare", "startingAirport", "destinationAirport", "flightDate") \
                            .orderBy(col("totalFare").asc()) \
                            .limit(3)
        flights_on_date_data = flights_on_date.collect()
        for row in flights_on_date_data:
            suggestions_str += (f"Flight: {row['legId']} | Fare: {row['totalFare']} | Date: {row['flightDate']} | "
                                f"Starting Airport: {row['startingAirport']} | Destination Airport: {row['destinationAirport']}\n")
    if starting_airport:
        suggestions_str += "\nTop 3 cheapest flights from the specified starting airport:\n"
        flights_from_airport = df.filter(col("startingAirport") == starting_airport) \
                                .select("legId", "totalFare", "startingAirport", "destinationAirport", "flightDate") \
                                .orderBy(col("totalFare").asc()) \
                                .limit(3)
        flights_from_airport_data = flights_from_airport.collect()
        for row in flights_from_airport_data:
            suggestions_str += (f"Flight: {row['legId']} | Fare: {row['totalFare']} | Date: {row['flightDate']} | "
                                f"Starting Airport: {row['startingAirport']} | Destination Airport: {row['destinationAirport']}\n")

    elapsed_time = f"\nElapsed time: {time.time() - start_time:.2f} seconds\n"
    output = result_str + suggestions_str + elapsed_time
    
    bucket_name = 'practice1-436812_bucket' 
    file_name = 'output_project/flight_price_analysis_output.txt'  
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    blob.upload_from_string(output)

# Start analysis
analyze_prices(starting_airport="LGA", destination_airport="DTW", flight_date="2022-05-27")

spark.stop()
