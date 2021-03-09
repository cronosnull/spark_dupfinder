#!/usr/env python
import sys
import os
from pathlib import Path
import hashlib
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    udf,
    pandas_udf,
    explode,
    col,
    from_json,
    from_unixtime,
    max as _max,
    count,
    collect_list,
)
from pyspark.sql.types import LongType, StringType
from pyspark import StorageLevel
import pandas as pd
import click

stat_f = {
    "st_mode": "int",
    "st_ino": "long",
    "st_nlink": "int",
    "st_uid": "int",
    "st_gid": "int",
    "st_size": "long",
    "st_atime": "double",
    "st_mtime": "double",
    "st_ctime": "double",
}


def create_spark_session():
    return SparkSession.builder.appName("findDuplicates").getOrCreate()


@udf(StringType())
def sha256sum(filename):
    h = hashlib.sha256()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    try:
        with open(filename, "rb", buffering=0) as f:
            for n in iter(lambda: f.readinto(mv), 0):
                h.update(mv[:n])
        return h.hexdigest()
    except Exception as e:
        return None


@udf(StringType())
def get_stats_as_dict(name):

    st = None
    try:
        st = os.stat(name, follow_symlinks=False) if not os.path.islink(name) else None
    except OSError as e:
        print(f"error reading {name}, {e}")
    return json.dumps({k: st.__getattribute__(k) for k in stat_f}) if st else None


def find_duplicates(spark, input_folder, min_size=2000000):
    files_df = (
        spark.read.text(input_folder)
        .withColumnRenamed("value", "name")
        .dropDuplicates()
        .repartition(20)
    )
    # files_df = spark.read.text("filelist.txt.gz").withColumnRenamed("value","name").dropDuplicates().repartition(20)
    files_df = files_df.withColumn("stats", get_stats_as_dict("name"))
    files_df = files_df.withColumn(
        "stats", from_json("stats", ",".join([f"{k} {stat_f[k]}" for k in stat_f]))
    ).where(f"stats.st_size>{min_size}")
    files_df = files_df.withColumn("hash", sha256sum("name"))
    files_df = files_df.select("name", "hash", "stats.*")
    for k in stat_f:
        if "time" in k:
            files_df = files_df.withColumn(k, from_unixtime(col(k)))
    duplicates = (
        files_df.where(f"hash is not null")
        .groupBy("hash", "st_size")
        .agg(count("name").alias("count"), collect_list("name"))
        .where("count>1")
    )
    dups_df = duplicates.withColumn(
        "wasted_space", col("st_size") * (col("count") - 1)
    ).toPandas()
    return dups_df.sort_values("wasted_space", ascending=False)


@click.command()
@click.argument("input_folder", nargs=1)
@click.option("-o", "--output_file", default="output.csv")
@click.option(
    "-s", "--min_size", default=2 * 1024 * 1024, help="Min file size in bytes"
)
def main(input_folder, output_file, min_size):
    spark = create_spark_session()
    dups_pdf = find_duplicates(spark, input_folder, min_size)
    dups_pdf.to_csv(output_file, index=False)


if __name__ == "__main__":
    main()
