from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType
import pyspark.sql.functions as f
import os

spark = (SparkSession
         .builder
         .master("local")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0")
         .config("spark.sql.parquet.writeLegacyFormat", True)
         .getOrCreate())

#%%

TARGET_DIR = os.path.abspath("../chinook")

#%%

artist_df = spark.read.csv(os.path.abspath("../chinook.csv/Artist.csv"), header=True)
artist_df = artist_df.withColumn("ArtistId", f.col("ArtistId").cast(IntegerType())).withColumn("Name", f.col("Name").cast(StringType()))
artist_df.printSchema()
artist_df.show()

#%%

# write single partition (ArtistSimple

(artist_df
 .repartition(1).write
 .format("delta")
 .mode("overwrite")
 .save(os.path.join(TARGET_DIR, "artist")))


#%%

# df.repartition(1).write.format("parquet").mode("overwrite").save("C:/dev/dd/delta-dotnet/src/Delta.Net.Test/data/golden/data-reader-array-primitives.parquet")
