# Databricks notebook source
# DBTITLE 1,Cell 1
#dbutils.fs.rm("abfss://silver@arxivetldevsa.dfs.core.windows.net/_checkpoints/papers_silver", recurse=True)

# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %sql
# MAGIC ---DROP TABLE IF EXISTS main.arxiv.papers_enriched;
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# COMMAND ----------

# DBTITLE 1,Cell 4
# Unity Catalog External Location akan  menghandle authentication otomatis.
bronze_path = "abfss://bronze@arxivetldevsa.dfs.core.windows.net/arxiv/"
 
# The Unity Catalog table for Silver layer
catalog_name = "main"
schema_name  = "arxiv"
silver_table = f"{catalog_name}.{schema_name}.papers_silver"
 
# Checkpoint lokasi untuk Auto Loader (untuk mengingat file mana yang sudah diproses sebelumnya)
unity_catalog_root = f"abfss://catalog-storage@arxivetldevsa.dfs.core.windows.net/"
checkpoint_path = f"abfss://silver@arxivetldevsa.dfs.core.windows.net/_checkpoints/papers_silver/"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name} MANAGED LOCATION '{unity_catalog_root}'")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{schema_name}")

# COMMAND ----------

bronze_schema = StructType([
    StructField("id",               StringType(),           nullable=False),
    StructField("title",            StringType(),           nullable=False),
    StructField("summary",          StringType(),           nullable=True),
    StructField("authors",          ArrayType(StringType()), nullable=True),
    StructField("published",        StringType(),           nullable=False),
    StructField("primary_category", StringType(),           nullable=True),
    StructField("categories",       ArrayType(StringType()), nullable=True),
])

# COMMAND ----------

raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("multiline", "true")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
    .schema(bronze_schema)
    .load(bronze_path)
)

# COMMAND ----------

silver_stream = (
    raw_stream
    .withColumn("published_ts", F.to_timestamp("published", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .withColumn("year_month", F.date_format("published_ts", "yyyy-MM"))
    .withColumn("arxiv_id_with_version", F.regexp_extract("id", r"/abs/(.+)$", 1))
    .withColumn("arxiv_id", F.regexp_replace("arxiv_id_with_version", r"v\d+$", ""))
    .drop("arxiv_id_with_version")
    .withColumn("authors_str", F.concat_ws(", ", F.col("authors")))
    .withColumn("categories_str", F.concat_ws(", ", F.col("categories")))
    .withColumn("ingested_at", F.current_timestamp())
)

# COMMAND ----------

# DBTITLE 1,Cell 8
from delta.tables import DeltaTable

def upsert_to_silver(microBatchDF, batchId):
    # Drop duplicates 
    deduped_df = (
        microBatchDF
        .dropDuplicates(["arxiv_id"])
        .withColumn("batch_id", F.lit(batchId))
    )
 
    # Cek jika table sudah ada di Unity Catalog
    table_exists = spark.catalog.tableExists(silver_table)
 
    if table_exists:
        # Perform UPSERT (Merge) dengan DeltaTable
        target_delta_table = DeltaTable.forName(spark, silver_table)
        
        (
            target_delta_table.alias("target")
            .merge(
                deduped_df.alias("source"),
                "target.arxiv_id = source.arxiv_id" # Match condition
            )
            .whenMatchedUpdateAll() # Updates existing records
            .whenNotMatchedInsertAll() # Inserts new records
            .execute()
        )
    else:
        # Pertama jalankan: Buat tabel terlebih dahulu
        (
            deduped_df.write
            .format("delta")
            .partitionBy("batch_id")
            .mode("overwrite")
            .option("path", "abfss://silver@arxivetldevsa.dfs.core.windows.net/arxiv/papers_silver")
            .saveAsTable(silver_table)
        )

# COMMAND ----------

# DBTITLE 1,Cell 9
query = (
    silver_stream.writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
)
 
query.awaitTermination()
 
print(f"Bronze to Silver Auto Loader complete for Unity Catalog table: {silver_table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.arxiv.papers_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY main.arxiv.papers_silver;