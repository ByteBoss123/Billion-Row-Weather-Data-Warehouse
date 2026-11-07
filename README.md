# Billion-Row Weather Data Warehouse

**Stack:** PySpark, Spark SQL, AWS EMR Serverless, Amazon S3, Parquet, pytest

## Business Problem
Insurance, energy, agriculture and climate research all draw on NOAA's daily weather record:
**3.26 billion observations from 132,894 stations going back to 1750.** NOAA publishes it as 265
yearly CSV files totalling 114 GB. In that form every question, even "what was the average high
temperature in 2023?", has to read all 114 GB, rows that failed NOAA's own quality control sit
mixed in with good data, and nothing guarantees a row was not dropped or duplicated on the way in.
Analysts need a warehouse that answers in seconds and that they can trust.

## Steps Taken to Resolve
1. **Ingested the full record:** read all 265 files from NOAA's public S3 bucket with a PySpark job on
   AWS EMR Serverless (10 executors), after capturing the file list, sizes and checksums for provenance.
2. **Typed and validated every row:** station id, date, element and value are typed; rows NOAA flagged
   as failing quality control, or with a bad id, date or value, go to a quarantine table instead of being deleted.
3. **Modeled for queries:** wrote a fact table partitioned by year (265 partitions, Parquet) and a station
   dimension parsed from NOAA's fixed-width station list.
4. **Proved nothing was lost:** automated checks for row conservation (source = loaded + quarantined),
   orphan stations, duplicate station-date-element rows and year range; any failure is recorded as a fail.
5. **Measured the payoff:** ran the same 2023 question against the raw CSV and the warehouse and
   compared both the time and the answer.
6. **Tested the rules first:** a 676-row sample in NOAA's format with deliberately bad rows confirms
   each rule catches what it should before running on 3 billion rows.

## Achievements
- Loaded **3,258,222,623 real rows (114 GB)** in **19 minutes**, with **0 rows lost**: 3,249,074,450 loaded
  plus 9,148,173 quarantined equals the source exactly.
- Quarantined **9.1M rows (0.28%)** that failed NOAA quality control, led by internal-consistency (6.3M)
  and duplicate (1.2M) flags, so they never reach analysts.
- Cut the 2023 query from **148 s to 2.9 s (51x faster)** with the **identical answer** (4,434,467 rows,
  16.957 °C average high).
- Shrank storage from **114 GB of CSV to 12.2 GB of Parquet (9.4x)**.
- All checks passed: **0 orphan stations, 0 duplicates**; whole run 27 minutes for about **$1.54**.

## Data
GHCN-Daily, NOAA version 3.35-upd-2026110417: `s3://noaa-ghcn-pds/csv/by_year/`, 265 files, 114.1 GB,
1750 to 2026, 144 elements (temperature, precipitation, snow and others). NOAA updates these files
daily, so a rerun will see slightly more rows.

## Pipeline (`pipeline/ghcn_pipeline.py`)

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
spark-submit pipeline/ghcn_pipeline.py s3://noaa-ghcn-pds/csv/by_year s3://noaa-ghcn-pds/ghcnd-stations.txt s3://<your-bucket>/ghcn/out 2023
```
The job role needs read access to `noaa-ghcn-pds` and write access to the output bucket.

## Data credit

Menne, M.J., et al. (2012): Global Historical Climatology Network - Daily (GHCN-Daily), Version 3.
NOAA National Centers for Environmental Information. Accessed through the NOAA Open Data
Dissemination program on AWS.
