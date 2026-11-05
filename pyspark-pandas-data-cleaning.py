"""Seven-step data cleaning pipeline using the pandas API on Spark.

Run from the repository root:
    python pyspark_pandas_data_cleaning.py

Spark writes CSV output to a directory containing one or more part files.
"""

from pathlib import Path

import pyspark.pandas as ps
from pyspark.sql import SparkSession

INPUT_PATH = "data/orders.csv"
OUTPUT_PATH = "output/orders_clean"
OUTLIER_MULTIPLIER = 1.5


def clean_orders(input_path: str = INPUT_PATH, output_path: str = OUTPUT_PATH) -> None:
    """Clean order data and export it using the pandas API on Spark."""
    spark = (
        SparkSession.builder
        .appName("PandasOnSparkDataCleaning")
        .master("local[*]")
        .getOrCreate()
    )
    try:
        # 1. Inspect data quality.
        df = ps.read_csv(input_path)
        required = {"order_id", "customer", "city", "price", "qty"}
        missing_columns = required.difference(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

        print("Original rows:", len(df))
        print("Missing values by column:\n", df.isna().sum())
        print("Duplicate rows:", int(df.duplicated().sum()))
        print("Data types:\n", df.dtypes)

        # 2. Remove fully duplicated rows.
        df = df.drop_duplicates()

        # 3. Trim whitespace; preserve nulls rather than turning them into text.
        for column in ("customer", "city"):
            df[column] = df[column].str.strip()
            df[column] = df[column].replace("", None)

        # 4. Convert numeric columns; invalid entries become NaN.
        for column in ("price", "qty"):
            df[column] = ps.to_numeric(df[column], errors="coerce")

        # 5. Drop rows missing essential fields; fill optional values.
        df = df.dropna(subset=["order_id", "customer", "price"])
        df = df.fillna({"city": "Unknown", "qty": 0})

        # 6. Replace price outliers using the IQR rule and median.
        # For large distributed datasets, quantiles may be approximate.
        q1 = df["price"].quantile(0.25)
        q3 = df["price"].quantile(0.75)
        median_price = df["price"].median()
        if q1 is not None and q3 is not None and median_price is not None:
            iqr = q3 - q1
            lower = q1 - OUTLIER_MULTIPLIER * iqr
            upper = q3 + OUTLIER_MULTIPLIER * iqr
            df["price"] = df["price"].where(
                (df["price"] >= lower) & (df["price"] <= upper),
                median_price,
            )

        # 7. Sort and export. The output path is a DIRECTORY of CSV part files.
        df = df.sort_values("order_id")
        df.to_csv(output_path, index_col=None, header=True, mode="overwrite")
        print("Cleaned rows:", len(df))
        print("CSV output directory:", output_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    if not Path(INPUT_PATH).is_file():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_PATH}. Add your CSV before running."
        )
    clean_orders()
