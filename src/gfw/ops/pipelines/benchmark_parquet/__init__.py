"""Benchmark query performance of a native BigQuery table vs a Parquet external table."""
from gfw.ops.pipelines.benchmark_parquet import main


run = main.run
