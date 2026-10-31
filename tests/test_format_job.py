import csv

import pytest
from pyspark.sql import functions as F

from benchmarks import format_job as J
from benchmarks import formats
from benchmarks.output_checksum import checksum
from pipeline import schemas as S
from pipeline.ingest import read_csv, to_bronze_interactions

DAY_MS = 86_400_000
# 2022-05-01 00:30 Beijing time; events span 2022-04-29 to 2022-05-10 local dates.
T0 = 1_651_336_200_000


@pytest.fixture
def sources(spark, tmp_path):
    fields = [f.name for f in S.RAW_LOG.fields]
    path = tmp_path / "log_x.csv"
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fields)
        writer.writeheader()
        for i in range(600):
            row = dict.fromkeys(fields, i % 3)
            row.update(user_id=[J.USER, 7, 900][i % 3], video_id=i, date=20220501, hourmin=0,
                       time_ms=T0 + (i % 12 - 2) * DAY_MS + i * 1000, play_time_ms=i * 10, duration_ms=5000)
            writer.writerow(row)
    # The cluster's default filesystem is HDFS, so local files need file://.
    from_csv = to_bronze_interactions(read_csv(spark, f"file://{path}", S.RAW_LOG))
    from_csv.write.partitionBy("event_date").parquet(f"file://{tmp_path}/bronze")
    return from_csv, spark.read.parquet(f"file://{tmp_path}/bronze")


def test_queries_give_the_same_result_on_csv_and_parquet(sources):
    from_csv, from_parquet = sources
    assert list(J.QUERIES) == formats.QUERIES
    for name, query in J.QUERIES.items():
        assert checksum(query(from_csv)) == checksum(query(from_parquet)), name


def test_filters_select_something_but_not_everything(sources):
    from_csv, _ = sources
    days = from_csv.filter(F.col("event_date").between(*J.DATE_RANGE)).count()
    assert 0 < days < 600 and from_csv.filter(F.col("user_id") == J.USER).count() == 200
    assert J.two_columns(from_csv).count() == 3
