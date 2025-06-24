import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def process_user_transaction_analysis(users_path, transactions_path, output_path):
    
    spark = SparkSession.builder.appName("User-Transaction-Analysis").getOrCreate()
    
    try:
        # Load datasets
        users_df = spark.read.option("header", "true").csv(users_path)
        transactions_df = spark.read.option("header", "true").csv(transactions_path)
        
        # Data type conversions
        transactions_df = transactions_df.withColumn("total_amount", col("total_amount").cast(DoubleType())) \
                                       .withColumn("rental_start_time", to_timestamp(col("rental_start_time"))) \
                                       .withColumn("rental_end_time", to_timestamp(col("rental_end_time"))) \
                                       .withColumn("user_id", col("user_id").cast(IntegerType()))
        
        # Calculate rental duration in hours
        transactions_df = transactions_df.withColumn(
            "rental_duration_hours", 
            (unix_timestamp("rental_end_time") - unix_timestamp("rental_start_time")) / 3600
        )
        
        # Extract date for daily analysis
        transactions_df = transactions_df.withColumn("rental_date", to_date("rental_start_time"))
        
        # FILE 1: Daily Transaction Metrics (All daily KPIs in one table)
        daily_transaction_metrics = transactions_df.groupBy("rental_date") \
            .agg(
                count("*").alias("total_transactions_per_day"),
                sum("total_amount").alias("revenue_per_day"),
                avg("total_amount").alias("average_transaction_value"),
                max("total_amount").alias("max_transaction_amount_daily"),
                min("total_amount").alias("min_transaction_amount_daily"),
                countDistinct("user_id").alias("unique_users_per_day")
            ) \
            .orderBy("rental_date")
        
        # FILE 2: User Engagement Metrics (All user KPIs in one table)
        user_engagement_metrics = transactions_df.groupBy("user_id") \
            .agg(
                sum("total_amount").alias("user_total_spending"),
                count("*").alias("user_total_transactions"),
                avg("total_amount").alias("user_average_spending"),
                sum("rental_duration_hours").alias("user_total_rental_hours"),
                avg("rental_duration_hours").alias("user_average_rental_duration"),
                max("total_amount").alias("user_max_transaction_amount"),
                min("total_amount").alias("user_min_transaction_amount")
            )
        
        # Write only 2 files
        daily_transaction_metrics.write.mode("overwrite").parquet(f"{output_path}/daily_transaction_metrics")
        user_engagement_metrics.write.mode("overwrite").parquet(f"{output_path}/user_engagement_metrics")
        
        print("User and Transaction Analysis completed!")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--users_path', default='s3://emr.rawdata/users/')
    parser.add_argument('--transactions_path', default='s3://emr.rawdata/rental_transactions/')
    parser.add_argument('--output_path', default='s3://emr.processed/job2/')
    
    args = parser.parse_args()
    
    process_user_transaction_analysis(
        args.users_path,
        args.transactions_path,
        args.output_path
    )

