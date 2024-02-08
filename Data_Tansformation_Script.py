from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit,dense_rank
from pyspark.sql.window import Window
from datetime import datetime

current_datetime = datetime.now()
current_time_stamp = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("S3 to Parquet") \
    .getOrCreate()

def read_the_data_content_create_dataframe_sources(website_data_path,
                                                   mobile_data_path,
                                                   oltp_data_path,
                                                   webhook_data_path):

    try:
        website_df = spark.read.json.load(f's3a://{website_data_path}')
        mobile_df = spark.read.csv.load(f's3a://{mobile_data_path}')
        oltp_df = spark.read.csv.load(f's3a://{oltp_data_path}')
        webhook_df = spark.read.json.load(f's3a://{webhook_data_path}')
        return website_df, mobile_df, oltp_df, webhook_df
    except Exception as e:
        print(e)
        return None, None, None, None

def transformation_and_join_df(df1, df2, df3, df4):
    df1 = df1.withColumn('created', lit(current_time_stamp)).withColumn('modified', lit(current_time_stamp))
    df2 = df2.withColumn('created', lit(current_time_stamp)).withColumn('modified', lit(current_time_stamp))
    df3 = df3.withColumn('created', lit(current_time_stamp)).withColumn('modified', lit(current_time_stamp))
    df4 = df4.withColumn('created', lit(current_time_stamp)).withColumn('modified', lit(current_time_stamp))

    windowspec = window.partition('user_id', 'tracking_id').sort_by('cdc_time_stamp').desc()
    df1 = df1.withColumn('rank', dense_rank().over(window_spec)).filter(col('rank') ==1).drop(col('rank'))
    df2 = df2.withColumn('rank', dense_rank().over(window_spec)).filter(col('rank') ==1).drop(col('rank'))
    df3 = df3.withColumn('rank', dense_rank().over(window_spec)).filter(col('rank') ==1).drop(col('rank'))
    df4 = df4.withColumn('rank', dense_rank().over(window_spec)).filter(col('rank') ==1).drop(col('rank'))

    json_joined_df = df1.join(df4, on=['user_id', 'tracking_id'], how='inner')
    csv_joined_df = df2.join(df3, on=['user_id', 'tracking_id'], how='inner')
    final_df = json_joined_df.join(csv_joined_df, on=['user_id', 'tracking_id'], how='inner')
    return final_df

def write_df_parquet_s3(df, processed_path):
    processed_path = f'{processed_path}{current_time_stamp}.parquet'
    df.colesce(1).write.partitionBy('user_id', 'tracking_id').parquet(processed_path, mode='merge')


def main():
    website_data_path = 'data-lake-devb/raw_layer/website/*.json'
    mobile_data_path = 'data-lake-devb/raw_layer/mobile/*.csv'
    oltp_data_path = 'data-lake-devb/raw_layer/oltp/*.csv'
    webhook_data_path = 'data-lake-devb/raw_layer/webhook/*.json'
    processed_path = 'data-lake-devb/processed_layer/'

    df1, df2, df3, df4 = read_the_data_content_create_dataframe_sources(website_data_path,
                                                   mobile_data_path,
                                                   oltp_data_path,
                                                   webhook_data_path)
    processed_data_frame = transformation_and_join_df(df1, df2, df3, df4)
    
    write_df_parquet_s3(processed_data_frame, processed_path)
    
if __name__ == "__main__":
    main()    


    
