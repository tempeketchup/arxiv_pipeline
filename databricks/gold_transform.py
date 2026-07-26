# Databricks notebook source
 
from pyspark.sql import functions as F
from delta.tables import DeltaTable
 
def rows_written(table_name):
    history = DeltaTable.forName(spark, table_name).history(1)
    metrics = history.select("operationMetrics").collect()[0][0]
    return metrics.get("numOutputRows", "unknown")

# COMMAND ----------

catalog_name  = "main"
schema_name   = "arxiv"
silver_table  = f"{catalog_name}.{schema_name}.papers_silver"
 
gold_trends_table   = f"{catalog_name}.{schema_name}.category_trends"
gold_enriched_table = f"{catalog_name}.{schema_name}.papers_enriched"

# COMMAND ----------

silver_df = spark.table(silver_table)
print(f"Read Silver Table: {silver_table}")

# COMMAND ----------

category_trends_df = (
    silver_df
    .filter(F.col("primary_category").isNotNull())
    .groupBy("year_month", "primary_category")
    .agg(
        # Karena Silver sudah dideduplikasi dengan MERGE, paper_count dan unique_papers akan otomatis sama. 
        F.count("*").alias("paper_count"),
        F.countDistinct("arxiv_id").alias("unique_papers"),
    )
    .orderBy("year_month", F.desc("paper_count"))
)

(
    category_trends_df.write
    .format("delta")
    .mode("overwrite")
    .option("path", "abfss://gold@arxivetldevsa.dfs.core.windows.net/arxiv/category_trends")
    .saveAsTable(gold_trends_table)
)
print(f"Updated Gold Table: {gold_trends_table} ({rows_written(gold_trends_table)} rows written)")

# COMMAND ----------

papers_enriched_df = (
    silver_df
    .select(
        "arxiv_id", "title", "summary",
        "authors_str", "primary_category", "categories_str",
        "published_ts", "year_month", "ingested_at"
    )
)
 
# Timpa Gold table di Unity Catalog
(
    papers_enriched_df.write
    .format("delta")
    .mode("overwrite")
    .option("path", "abfss://gold@arxivetldevsa.dfs.core.windows.net/arxiv/papers_enriched")
    .saveAsTable(gold_enriched_table)
)
print(f"Updated Gold Table: {gold_enriched_table} ({rows_written(gold_enriched_table)} rows written)")
 
print("Silver -> Gold transform complete.")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM main.arxiv.papers_enriched