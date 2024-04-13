import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql import SparkSession, DataFrame

def create_spark_session(gcs_bucket: str = None) -> SparkSession:
    """
    Spark session creation
    :gcs_bucket: The GCS bucket for temporary BigQuery export data (optional).
    :return: A SparkSession instance.
    """
    spark = SparkSession.builder.master('yarn').appName("mmd-spark").getOrCreate()
    spark.conf.set('spark.sql.legacy.timeParserPolicy', 'LEGACY')

    if gcs_bucket:
        spark.conf.set('temporaryGcsBucket', gcs_bucket)
    return spark

def correctSchema():
    """
    schema for spark to transform parquet files
    :return: the schema
    """
    schema = StructType([StructField("gadm_id",StringType(),False),\
                        StructField("gadm_name",StringType(),False),\
                        StructField("country",StringType(),True),\
                        StructField("polygon_level",IntegerType(),False),\
                        StructField("home_to_ping_distance_category",StringType(),False),\
                        StructField("distance_category_ping_fraction",DoubleType(),False),\
                        StructField("ds",DateType(),False)])
    return schema

def read_parquet_data(spark: SparkSession, path: str) -> DataFrame:
    """
    Create dataframe from Parquet files.
    :param spark: A SparkSession object.
    :param path: The path to the Parquet file.
    :return: A DataFrame with the Parquet data.
    """
    return spark.read.format("parquet").option("mergeSchema", "true").load(path)


bucket = "mmd-bucket_meta-movement-dist" #['bucket']
spark = create_spark_session(bucket)

df = read_parquet_data(spark, f"gs://{bucket}/data/*.parquet")
df = df.withColumn(polygon_level, col(polygon_level).cast(IntegerType()))

contry_df = read_parquet_data(spark, f"gs://{bucket}/code/country.csv")


# filter out long distances and noise fraction distributions
df = df.filter(df['home_to_ping_distance_category'] == "100+") \
        .filter(df['distance_category_ping_fraction'] > 0) \
        .join(country_df, on=[country_df['country_iso2'] == df['country']])    




#write to BQ
df.write.format('bigquery') \
    .option('table', f'{dataset_id}.mmd_table') \
    .mode('append') \
    .option("partitionField", "ds") \
    .option("clusteredFields", "country") \
    .save()
