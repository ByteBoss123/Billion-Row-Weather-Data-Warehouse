"""GHCN-Daily (NOAA) billion-row warehouse build on Spark.

Reads every by-year CSV from the public NOAA bucket, types and validates each observation,
quarantines rows that fail NOAA's own quality control or our parsing, writes a year-partitioned
Parquet fact table and a station dimension, runs dataset checks, and benchmarks a partition-pruned
query against the same query on the raw CSV. Writes results.json next to the outputs.

Args: <source_prefix> <stations_path> <output_prefix> [benchmark_year]
"""
import json, sys, time
from pyspark.sql import SparkSession, functions as F, types as T

src, stations_path, out = sys.argv[1].rstrip("/"), sys.argv[2], sys.argv[3].rstrip("/")
bench_year = int(sys.argv[4]) if len(sys.argv) > 4 else 2023
spark = SparkSession.builder.appName("ghcn-billion-row-warehouse").getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
R, t0 = {"source": src, "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}, time.time()

def put_results():
    R["elapsed_s"] = round(time.time() - t0, 1)
    spark.sparkContext.parallelize([json.dumps(R, indent=1)], 1).saveAsTextFile(f"{out}/results_{int(time.time())}")

SCHEMA = T.StructType([T.StructField(c, T.StringType()) for c in
                       ["ID", "DATE", "ELEMENT", "DATA_VALUE", "M_FLAG", "Q_FLAG", "S_FLAG", "OBS_TIME"]])
raw = spark.read.csv(f"{src}/*.csv", header=True, schema=SCHEMA, mode="PERMISSIVE")

typed = (raw
         .withColumn("obs_date", F.to_date("DATE", "yyyyMMdd"))
         .withColumn("value", F.col("DATA_VALUE").cast("int"))
         .select(F.col("ID").alias("station_id"), "obs_date", F.col("ELEMENT").alias("element"), "value",
                 F.col("M_FLAG").alias("m_flag"), F.col("Q_FLAG").alias("q_flag"), F.col("S_FLAG").alias("s_flag"),
                 F.col("OBS_TIME").alias("obs_time"), F.input_file_name().alias("_file"))
         .withColumn("reject_reason",
                     F.when(F.col("station_id").isNull() | (F.length("station_id") != 11), "bad_station_id")
                      .when(F.col("obs_date").isNull(), "bad_date")
                      .when(F.col("element").isNull(), "missing_element")
                      .when(F.col("value").isNull(), "bad_value")
                      .when(F.col("value") == -9999, "missing_value_sentinel")
                      .when(F.col("q_flag").isNotNull(), "failed_noaa_qc"))
         .withColumn("year", F.year("obs_date")))

# 1. Ingest: one pass writes both outputs; counts come from the written data.
s = time.time()
(typed.filter("reject_reason is null").drop("reject_reason", "_file")
      .repartition("year").write.mode("overwrite").partitionBy("year").parquet(f"{out}/fact_observation"))
(typed.filter("reject_reason is not null").drop("year")
      .write.mode("overwrite").parquet(f"{out}/quarantine"))
R["ingest_s"] = round(time.time() - s, 1)

fact = spark.read.parquet(f"{out}/fact_observation")
quar = spark.read.parquet(f"{out}/quarantine")
s = time.time()
src_rows = spark.read.csv(f"{src}/*.csv", header=True, schema=SCHEMA).count()
R["source_count_s"] = round(time.time() - s, 1)
fact_rows, quar_rows = fact.count(), quar.count()
R["rows"] = {"source": src_rows, "fact": fact_rows, "quarantine": quar_rows}
R["quarantine_by_reason"] = {r["reject_reason"]: r["n"] for r in
                             quar.groupBy("reject_reason").agg(F.count("*").alias("n")).collect()}
R["qc_flags_failed"] = {r["q_flag"]: r["n"] for r in
                        quar.filter("reject_reason = 'failed_noaa_qc'").groupBy("q_flag").agg(F.count("*").alias("n")).collect()}

# 2. Station dimension from the fixed-width station file.
st = spark.read.text(stations_path)
dim = st.select(F.substring("value", 1, 11).alias("station_id"),
                F.trim(F.substring("value", 13, 8)).cast("double").alias("latitude"),
                F.trim(F.substring("value", 22, 9)).cast("double").alias("longitude"),
                F.trim(F.substring("value", 32, 6)).cast("double").alias("elevation_m"),
                F.trim(F.substring("value", 39, 2)).alias("state"),
                F.trim(F.substring("value", 42, 30)).alias("name"),
                F.substring("value", 1, 2).alias("country_code"))
dim.write.mode("overwrite").parquet(f"{out}/dim_station")
dim = spark.read.parquet(f"{out}/dim_station")

# 3. Dataset checks (each is pass/fail with the measured value).
fact_stations = fact.select("station_id").distinct()
orphans = fact_stations.join(dim, "station_id", "left_anti").count()
dupes = (fact.groupBy("station_id", "obs_date", "element").count().filter("count > 1")
             .agg(F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("extra")).collect()[0]["extra"])
yr = fact.agg(F.min("year").alias("lo"), F.max("year").alias("hi")).collect()[0]
dim_dupe_ids = dim.groupBy("station_id").count().filter("count > 1").count()
checks = {
    "row_conservation": {"value": src_rows - fact_rows - quar_rows, "pass": src_rows == fact_rows + quar_rows},
    "no_orphan_stations": {"value": orphans, "pass": orphans == 0},
    "unique_station_date_element": {"value": int(dupes), "pass": int(dupes) == 0},
    "station_dim_unique": {"value": dim_dupe_ids, "pass": dim_dupe_ids == 0},
    "years_in_range": {"value": [yr["lo"], yr["hi"]], "pass": yr["lo"] >= 1750 and yr["hi"] <= 2026},
}
R["checks"] = checks
R["profile"] = {"stations_with_data": fact_stations.count(), "stations_in_dim": dim.count(),
                "elements": fact.select("element").distinct().count(),
                "years": [yr["lo"], yr["hi"]]}
put_results()

# 4. Benchmark: the same question on raw CSV (must scan every file) vs year-partitioned Parquet.
def q_csv():
    return (spark.read.csv(f"{src}/*.csv", header=True, schema=SCHEMA)
            .filter((F.substring("DATE", 1, 4) == str(bench_year)) & (F.col("ELEMENT") == "TMAX") & F.col("Q_FLAG").isNull()
                    & F.col("DATA_VALUE").cast("int").isNotNull() & (F.col("DATA_VALUE") != "-9999")
                    & (F.length("ID") == 11) & F.to_date("DATE", "yyyyMMdd").isNotNull())
            .agg(F.count("*").alias("n"), F.avg(F.col("DATA_VALUE").cast("int") / 10).alias("avg_tmax_c")).collect()[0])
def q_parquet():
    return (spark.read.parquet(f"{out}/fact_observation").filter((F.col("year") == bench_year) & (F.col("element") == "TMAX"))
            .agg(F.count("*").alias("n"), F.avg(F.col("value") / 10).alias("avg_tmax_c")).collect()[0])
bench = {}
for name, fn, runs in [("raw_csv_full_scan", q_csv, 1), ("parquet_partition_pruned", q_parquet, 3)]:
    times, res = [], None
    for _ in range(runs):  # one raw scan (it reads all ~114 GB); three runs on the pruned table
        s = time.time(); res = fn(); times.append(round(time.time() - s, 2))
    bench[name] = {"seconds": times, "median_s": sorted(times)[len(times) // 2], "rows": res["n"], "avg_tmax_c": res["avg_tmax_c"]}
bench["year"] = bench_year
bench["same_answer"] = bench["raw_csv_full_scan"]["rows"] == bench["parquet_partition_pruned"]["rows"]
bench["speedup_x"] = round(bench["raw_csv_full_scan"]["median_s"] / bench["parquet_partition_pruned"]["median_s"], 1)
R["benchmark"] = bench
R["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
put_results()
print(json.dumps(R, indent=1))
