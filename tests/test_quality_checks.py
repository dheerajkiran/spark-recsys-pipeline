import pytest

from pipeline import quality_checks as qc
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def test_schema_mismatch_fails(spark):
    df = spark.createDataFrame([(1, "a")], "id int, name string")
    qc.check_schema(df, StructType([StructField("id", IntegerType()), StructField("name", StringType())]), "t")
    with pytest.raises(qc.DataQualityError, match="missing"):
        qc.check_schema(df, StructType([StructField("id", StringType()), StructField("name", StringType())]), "t")


def test_unique_fails_on_duplicate_keys(spark):
    df = spark.createDataFrame([(1, 1), (1, 2), (2, 1)], "a int, b int")
    qc.check_unique(df, ["a", "b"], "t")
    with pytest.raises(qc.DataQualityError):
        qc.check_unique(df, ["a"], "t")


def test_removed_fraction_limit():
    qc.check_removed_fraction(1000, 950, 0.10, "t")
    with pytest.raises(qc.DataQualityError):
        qc.check_removed_fraction(1000, 850, 0.10, "t")


def test_not_empty_and_equal_counts():
    with pytest.raises(qc.DataQualityError):
        qc.check_not_empty(0, "t")
    with pytest.raises(qc.DataQualityError):
        qc.check_equal_counts(10, 9, "t")


def test_range_and_nulls(spark):
    df = spark.createDataFrame([(0.5, 1), (None, 2), (1.5, None)], "r double, k int")
    with pytest.raises(qc.DataQualityError):
        qc.check_range(df, "r", 0.0, 1.0, "t")
    with pytest.raises(qc.DataQualityError):
        qc.check_no_nulls(df, ["k"], "t")
