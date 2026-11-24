"""One way to start a job, on AWS Glue or on a laptop.

On Glue, `awsglue` is importable: arguments come from getResolvedOptions and the job is
committed at the end (job bookmarks). Locally there is no `awsglue`, so arguments come from
argparse and a plain local SparkSession is used. The job logic itself (`run(spark, args)`)
is identical in both places.
"""

import argparse
import os
import sys
from typing import Callable

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def local_spark(app_name: str = "benefits-claims", kafka: bool = False):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )
    if kafka:
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGE)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def start_job(arg_names: list[str], defaults: dict, kafka: bool = False):
    """Return (spark, args, commit). Works on Glue and locally."""
    try:
        from awsglue.context import GlueContext
        from awsglue.job import Job
        from awsglue.utils import getResolvedOptions
        from pyspark.context import SparkContext
    except ImportError:
        parser = argparse.ArgumentParser()
        for name in arg_names:
            parser.add_argument(f"--{name}", default=defaults.get(name))
        args = vars(parser.parse_known_args()[0])
        return local_spark(kafka=kafka), args, lambda: None

    # getResolvedOptions fails on a missing argument, so only ask for the ones that were passed
    passed = [a for a in arg_names if f"--{a}" in sys.argv]
    args = {**defaults, **getResolvedOptions(sys.argv, ["JOB_NAME"] + passed)}
    sc = SparkContext.getOrCreate()
    glue = GlueContext(sc)
    spark = glue.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    job = Job(glue)
    job.init(args["JOB_NAME"], args)
    return spark, args, job.commit


def zone(root: str, *parts: str) -> str:
    """Build a path under the lake root. Works for s3://bucket/prefix and local folders."""
    return "/".join([root.rstrip("/")] + [p.strip("/") for p in parts])


def run_main(run: Callable, arg_names: list[str], defaults: dict, kafka: bool = False):
    spark, args, commit = start_job(arg_names, defaults, kafka=kafka)
    result = run(spark, args)
    commit()
    print(result)
    return result
