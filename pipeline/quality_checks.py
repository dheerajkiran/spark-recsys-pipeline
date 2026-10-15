"""Reusable data-quality gates. Each check prints PASS or raises DataQualityError.

Stages call these on what they actually wrote (read back from HDFS), so a
check failing means the stored data is wrong, not just an in-memory plan.
"""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType


class DataQualityError(Exception):
    pass


def _pass(name: str, detail: str) -> None:
    print(f"[PASS] {name}: {detail}")


def _fail(name: str, detail: str) -> None:
    raise DataQualityError(f"[FAIL] {name}: {detail}")


def check_schema(df: DataFrame, expected: StructType, name: str) -> None:
    """Column names, order and types must match; nullability is ignored."""
    actual = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
    wanted = [(f.name, f.dataType.simpleString()) for f in expected.fields]
    if actual != wanted:
        missing = [c for c in wanted if c not in actual]
        extra = [c for c in actual if c not in wanted]
        _fail(f"{name} schema", f"missing {missing}, unexpected {extra}"
              if missing or extra else f"column order differs: {actual}")
    _pass(f"{name} schema", f"{len(actual)} columns match")


def check_not_empty(count: int, name: str) -> None:
    if count <= 0:
        _fail(f"{name} not empty", "0 rows")
    _pass(f"{name} not empty", f"{count:,} rows")


def check_equal_counts(expected: int, actual: int, name: str) -> None:
    if expected != actual:
        _fail(name, f"expected {expected:,} rows, got {actual:,}")
    _pass(name, f"{actual:,} rows")


def check_unique(df: DataFrame, keys: list, name: str) -> None:
    dupes = df.groupBy(*keys).count().filter("count > 1").count()
    if dupes:
        _fail(f"{name} unique {keys}", f"{dupes:,} duplicated keys")
    _pass(f"{name} unique {keys}", "no duplicates")


def check_no_nulls(df: DataFrame, cols: list, name: str) -> None:
    nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).first()
    bad = {c: v for c, v in nulls.asDict().items() if v}
    if bad:
        _fail(f"{name} no nulls", str(bad))
    _pass(f"{name} no nulls", f"{len(cols)} columns")


def check_removed_fraction(before: int, after: int, max_fraction: float, name: str) -> None:
    """Guards against over-aggressive cleaning: fail if too many rows were dropped."""
    removed = (before - after) / before if before else 0.0
    if removed > max_fraction:
        _fail(f"{name} rows removed", f"{removed:.2%} > limit {max_fraction:.0%}")
    _pass(f"{name} rows removed", f"{before - after:,} ({removed:.2%}, limit {max_fraction:.0%})")


def check_range(df: DataFrame, col: str, low, high, name: str) -> None:
    """Non-null values of `col` must lie in [low, high]."""
    out = df.filter(F.col(col).isNotNull() & ((F.col(col) < low) | (F.col(col) > high))).count()
    if out:
        _fail(f"{name} {col} in [{low}, {high}]", f"{out:,} rows outside")
    _pass(f"{name} {col} in [{low}, {high}]", "all values in range")


def check_constant_within(df: DataFrame, keys: list, cols: list, name: str) -> None:
    """Every column must have a single value within each group of `keys`."""
    agg = df.groupBy(*keys).agg(*[F.countDistinct(F.col(c).cast("string")).alias(c) for c in cols])
    bad = agg.select([F.sum((F.col(c) > 1).cast("int")).alias(c) for c in cols]).first().asDict()
    bad = {c: v for c, v in bad.items() if v}
    if bad:
        _fail(f"{name} constant within {keys}", f"groups with differing values: {bad}")
    _pass(f"{name} constant within {keys}", f"{len(cols)} columns")
