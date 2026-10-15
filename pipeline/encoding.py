"""Categorical encoding: map raw values to contiguous integer indices for embeddings.

Index layout, shared by every vocabulary:
    0 = PAD  (padding in fixed-length history lists)
    1 = UNK  (rare in the source, unseen there, or null)
    2.. = known values, most frequent first

A vocabulary is a small table (value: string, idx: int, count: long). Values
are stored as strings so one format fits ints and strings alike.
"""

from pyspark.sql import DataFrame, Window, functions as F

PAD = 0
UNK = 1
FIRST = 2


def build_vocab(source: DataFrame, col: str, min_count: int) -> DataFrame:
    """Vocabulary of `col` over `source`; values seen fewer than min_count times map to UNK.

    The caller chooses the source, and with it what "seen" means: for ID
    features it is training-period events only, so val/test never shape it.
    """
    counts = (source.filter(F.col(col).isNotNull())
              .groupBy(F.col(col).cast("string").alias("value"))
              .count()
              .filter(F.col("count") >= min_count))
    # Ties broken by value so the same data always gives the same indices.
    order = Window.orderBy(F.desc("count"), F.asc("value"))
    return counts.select("value", (F.row_number().over(order) + (FIRST - 1)).cast("int").alias("idx"),
                         "count")


def apply_vocab(df: DataFrame, col: str, vocab: DataFrame, out_col: str) -> DataFrame:
    """Add out_col = index of df[col] in vocab, or UNK if absent or null."""
    v = vocab.select(F.col("value").alias("__v"), F.col("idx").alias(out_col))
    joined = df.join(v, df[col].cast("string") == v["__v"], "left").drop("__v")
    return joined.withColumn(out_col, F.coalesce(F.col(out_col), F.lit(UNK)).cast("int"))


def vocab_size(vocab: DataFrame) -> int:
    """Embedding table size: max index + 1 (covers PAD and UNK even when empty)."""
    top = vocab.agg(F.max("idx")).first()[0]
    return int(top if top is not None else UNK) + 1
