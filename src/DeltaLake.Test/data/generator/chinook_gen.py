import os

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType

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

def read_csv(name: str):
    return (spark.read
     .option("header", "true")
     .option("quote", "\"")
     .option("escape", "\"")
     .csv(os.path.abspath(f"../chinook.csv/{name}.csv")))

# Artist table
artist_df = read_csv("Artist")
track_df = read_csv("Track")
playlist_df = read_csv("Playlist")
playlist_track_df = read_csv("PlaylistTrack")
artist_df = (artist_df
             .withColumn("ArtistId", f.col("ArtistId").cast(IntegerType()))
             .withColumn("Name", f.col("Name").cast(StringType())))

# add row number to track_df
track_df_rn = (track_df
            .withColumn("rn", f.monotonically_increasing_id())
            .select("rn", *track_df.columns))
track_df_count = track_df.count()

# add row number column to artist_df with increasing values, this will be useful for values partitioning and data generation
artist_df_rn = (artist_df
                .withColumn("rn", f.monotonically_increasing_id())
                .select("rn", *artist_df.columns))
artist_df_count = artist_df.count()


#%%

# single file and single partition (ArtistSimple)

(artist_df
 .repartition(1).write
 .format("delta")
 .mode("overwrite")
 .save(os.path.join(TARGET_DIR, "artist.simple")))


#%%

# trickling rows

batch_size = 20

for i in range(1, artist_df_count + 1, batch_size):
    # generate microbatch
    df_1 = artist_df_rn.filter(artist_df_rn.rn >= i).filter(artist_df_rn.rn < i + batch_size)
    df_1.show()
    df_1 = df_1.drop("rn")

    # write microbatch
    print("-----")
    print(f"writing microbatch {i}+{batch_size}/{artist_df_count}")
    (df_1
     .repartition(1)
     .write
     .format("delta")
     .mode("append" if i > 1 else "overwrite")
     .save(os.path.join(TARGET_DIR, "artist.trickle")))

print("done")

#%%

(track_df
 .write
 .partitionBy("MediaTypeId")
 .format("delta")
 .mode("overwrite")
 .save(os.path.join(TARGET_DIR, "track.partitioned.mediatypeid")))


#%%

# trickling partitioned data with checkpointing

batch_size = 100

for i in range(1, track_df_count + 1, batch_size):
    # generate microbatch
    df_1 = track_df_rn.filter(track_df_rn.rn >= i).filter(track_df_rn.rn < i + batch_size)
    df_1.show()
    df_1 = df_1.drop("rn")
    df_1.show()

    # write microbatch
    print("-----")
    print(f"writing microbatch {i}+{batch_size}/{track_df_count}")
    (df_1
     .repartition(1)
     .write
     .partitionBy("MediaTypeId")
     .format("delta")
     .mode("append" if i > 1 else "overwrite")
     .save(os.path.join(TARGET_DIR, "track.partitioned.mediatypeid.trickle")))

print("done")
