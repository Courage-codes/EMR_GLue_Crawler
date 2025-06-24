import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def process_vehicle_location_metrics(vehicles_path, locations_path, transactions_path, output_path):
    
    spark = SparkSession.builder.appName("Vehicle-Location-Performance-Metrics").getOrCreate()
    
    try:
        # Load datasets
        vehicles_df = spark.read.option("header", "true").csv(vehicles_path)
        locations_df = spark.read.option("header", "true").csv(locations_path)
        transactions_df = spark.read.option("header", "true").csv(transactions_path)
        
        # Data type conversions
        transactions_df = transactions_df.withColumn("total_amount", col("total_amount").cast(DoubleType())) \
                                       .withColumn("rental_start_time", to_timestamp(col("rental_start_time"))) \
                                       .withColumn("rental_end_time", to_timestamp(col("rental_end_time"))) \
                                       .withColumn("vehicle_id", col("vehicle_id").cast(IntegerType()))
        
        vehicles_df = vehicles_df.withColumn("vehicle_id", col("vehicle_id").cast(IntegerType()))
        
        # Calculate rental duration in hours
        transactions_df = transactions_df.withColumn(
            "rental_duration_hours", 
            (unix_timestamp("rental_end_time") - unix_timestamp("rental_start_time")) / 3600
        )
        
        # Join with locations
        location_transactions = transactions_df.join(
            locations_df, 
            transactions_df.pickup_location == locations_df.location_name, 
            "inner"
        )
        
        # Join with vehicles
        vehicle_transactions = transactions_df.join(
            vehicles_df, 
            transactions_df.vehicle_id == vehicles_df.vehicle_id, 
            "inner"
        )
        
        # FILE 1: Location Performance Metrics (All location KPIs in one table)
        location_performance_metrics = location_transactions.groupBy("pickup_location") \
            .agg(
                sum("total_amount").alias("revenue_per_location"),
                count("*").alias("total_transactions_per_location"),
                avg("total_amount").alias("average_transaction_amount"),
                max("total_amount").alias("max_transaction_amount"),
                min("total_amount").alias("min_transaction_amount"),
                countDistinct("vehicle_id").alias("unique_vehicles_used")
            )
        
        # FILE 2: Vehicle Type Performance Metrics (All vehicle KPIs in one table)
        vehicle_type_performance_metrics = vehicle_transactions.groupBy("vehicle_type") \
            .agg(
                sum("rental_duration_hours").alias("total_rental_duration"),
                avg("rental_duration_hours").alias("average_rental_duration"),
                sum("total_amount").alias("revenue_by_vehicle_type"),
                count("*").alias("total_transactions_by_vehicle_type"),
                avg("total_amount").alias("average_transaction_amount_by_vehicle_type")
            )
        
        # Write only 2 files
        location_performance_metrics.write.mode("overwrite").parquet(f"{output_path}/location_performance_metrics")
        vehicle_type_performance_metrics.write.mode("overwrite").parquet(f"{output_path}/vehicle_type_performance_metrics")
        
        print("Vehicle and Location Performance Metrics completed!")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--vehicles_path', default='s3://emr.rawdata/vehicles/')
    parser.add_argument('--locations_path', default='s3://emr.rawdata/locations/')
    parser.add_argument('--transactions_path', default='s3://emr.rawdata/rental_transactions/')
    parser.add_argument('--output_path', default='s3://emr.processed/job1/')
    
    args = parser.parse_args()
    
    process_vehicle_location_metrics(
        args.vehicles_path,
        args.locations_path, 
        args.transactions_path,
        args.output_path
    )

