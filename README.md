# Billion-Row Weather Data Warehouse

A Spark pipeline that loads **all 3,258,222,623 rows** of NOAA's Global Historical Climatology
Network Daily (GHCN-Daily), 1750 to 2026, from the public AWS bucket into a year-partitioned
Parquet warehouse, quarantines observations that failed NOAA's quality control, and checks the
result. Ran on AWS EMR Serverless on 2026-11-07.

**Data:** real station observations (temperature, precipitation, snow and 141 other elements) from
132,894 weather stations. Source `s3://noaa-ghcn-pds/csv/by_year/`, 265 files, 114.1 GB of CSV,
NOAA version 3.35-upd-2026110417 (file list with sizes and ETags captured at the start of the run).
NOAA updates these files daily, so a rerun will see slightly more rows.

## Pipeline (`jobs/ghcn_pipeline.py`)

```
114 GB CSV (265 yearly files)
  -> typed rows: station id, date, element, value, flags
  -> quarantine   rows NOAA flagged as failing QC, or bad id / date / value
  -> fact_observation   Parquet, partitioned by year (265 partitions)
  -> dim_station        parsed from NOAA's fixed-width station list
  -> checks + benchmark -> results.json
```

## Results (`results/results.json`)

| | |
|---|---|
| Source rows | 3,258,222,623 |
| Loaded to the fact table | 3,249,074,450 |
| Quarantined (failed NOAA quality control) | 9,148,173 (0.28%) |
| Largest QC failure types | I internal consistency 6.34M, D duplicate 1.20M, L multiday 0.40M |
| CSV in / Parquet out | 114.1 GB / 12.2 GB (9.4x smaller) |
| Ingest stage | 18.7 min; whole job 27.3 min |

Checks, all passed:

| Check | Result |
|---|---|
| Source rows = loaded + quarantined | difference 0 |
| Every station in the data exists in the station list | 0 orphans (132,894 of 133,041 stations have data) |
| One row per station, date and element | 0 duplicates |
| Station list has unique ids | 0 duplicates |
| Years within 1750 to 2026 | pass |

**Benchmark, same question both ways:** average 2023 maximum temperature over all stations.
Against the raw CSV the query must read all 114 GB; against the warehouse it reads one year partition.

| | Time | Rows | Answer |
|---|---|---|---|
| Raw CSV, full scan (1 run) | 148.1 s | 4,434,467 | 16.957 °C |
| Year-partitioned Parquet (median of 3) | 2.9 s | 4,434,467 | 16.957 °C |

**50.9x faster** with an identical answer.

## Cost and cleanup

EMR Serverless (emr-7.5.0, us-east-1): 10 executors x 4 vCPU / 14 GB plus a driver. Billed 19.6
vCPU-hours, 78.3 GB-hours of memory and 576 GB-hours of disk: about $1.54 at list price. The 12.2 GB
of Parquet output was deleted after the run; `results/results.json` records its file counts and sizes.

## Tests

`pytest tests` runs the same job locally on a 676-row fixture in GHCN format with injected bad rows
(bad station id, bad date, QC-flagged and missing values, a station absent from the station list)
and checks that each rule fires and the benchmark answers match. Needs `pyspark`.

## Run

```bash
spark-submit jobs/ghcn_pipeline.py s3://noaa-ghcn-pds/csv/by_year s3://noaa-ghcn-pds/ghcnd-stations.txt s3://<your-bucket>/ghcn/out 2023
```
The job role needs read access to `noaa-ghcn-pds` and write access to the output bucket.

## Data credit

Menne, M.J., et al. (2012): Global Historical Climatology Network - Daily (GHCN-Daily), Version 3.
NOAA National Centers for Environmental Information. Accessed through the NOAA Open Data
Dissemination program on AWS.
