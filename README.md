# PySpark Pandas Data Cleaning

A practical, step-by-step data cleaning workflow using **Pandas API on Spark (`pyspark.pandas`)**.

This project demonstrates how to inspect, clean, transform, and export datasets using a Pandas-like API powered by Apache Spark.

## P1 — Core Data Cleaning Workflow

### 1. Data Inspection

Check missing values, duplicate rows, and data types.

```python
df.isna().sum()
df.duplicated().sum()
df.dtypes
```

### 2. Remove Duplicates

Remove duplicate records from the dataset.

```python
df = df.drop_duplicates()
```

### 3. String Cleaning

Remove leading and trailing whitespace from text columns.

```python
df["customer"] = df["customer"].str.strip()
df["city"] = df["city"].str.strip()
```

### 4. Data Type Conversion

Convert string values to numeric types. Invalid values are converted to `NaN`.

```python
import pyspark.pandas as ps

df["price"] = ps.to_numeric(df["price"], errors="coerce")
df["qty"] = ps.to_numeric(df["qty"], errors="coerce")
```

### 5. Handle Missing Values

Remove rows with missing values in critical fields and fill missing values in other columns.

```python
df = df.dropna(subset=["order_id", "price"])

df["customer"] = df["customer"].fillna("Unknown")
df["qty"] = df["qty"].fillna(0)
```

### 6. Outlier Detection and Treatment

Identify unusually high prices and replace them with the median.

```python
median_price = df["price"].median()

df.loc[df["price"] > 10000, "price"] = median_price
```

The threshold of `10000` is an example business rule and should be adjusted according to the dataset.

### 7. Export Cleaned Data

Export the cleaned dataset to CSV.

```python
df.to_csv(
    "output/cleaned_orders",
    index_col=None,
    header=True
)
```

**Important:** Unlike standard Pandas, Spark typically writes distributed CSV output as multiple part files in an output directory.

## P2 — Important Notes and Best Practices

### DataFrame Modification

Methods such as `dropna()`, `fillna()`, and `drop_duplicates()` return a new DataFrame by default.

```python
df = df.dropna()
df = df.fillna(0)
df = df.drop_duplicates()
```

Reassign the result to preserve the changes.

### Pandas vs. Pandas API on Spark

| Feature | Pandas | Pandas API on Spark |
|---|---|---|
| Execution | Local | Distributed Spark engine |
| Main import | `import pandas as pd` | `import pyspark.pandas as ps` |
| DataFrame API | Pandas | Pandas-like |
| Large datasets | Limited by local memory | Can scale across a cluster |
| CSV export | Typically one file | Typically multiple part files |
| Row ordering | Generally preserved | Not guaranteed without explicit sorting |

### Best Practices

- Validate data types before performing calculations.
- Handle missing values according to business requirements.
- Avoid collecting large distributed datasets into local memory.
- Use explicit sorting when row order matters.
- Use `to_spark()` when native Spark DataFrame operations are required.
- Consider Parquet for efficient storage of large datasets.

## Installation

Install the required packages:

```bash
pip install "pyspark[pandas]"
```

A compatible Java runtime may also be required depending on the PySpark version and environment.

## Run the Project

```bash
python pyspark_pandas_data_cleaning.py
```

## Project Structure

```text
pyspark-pandas-data-cleaning/
├── README.md
├── requirements.txt
├── pyspark_pandas_data_cleaning.py
├── data/
│   └── orders.csv
└── output/
    └── cleaned_orders/
```

## Technologies

- Python
- Apache Spark
- Pandas API on Spark
- PySpark
- Distributed Data Processing
- Data Cleaning and Transformation

## Learning Objectives

Understand how to build reusable data cleaning workflows using Pandas API on Spark and prepare datasets for downstream analytics, reporting, and ETL pipelines.