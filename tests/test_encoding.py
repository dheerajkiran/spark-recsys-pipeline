from pipeline import encoding as E


def test_vocab_orders_by_count_and_starts_after_pad_and_unk(spark):
    df = spark.createDataFrame([("b",), ("a",), ("a",), ("c",), ("c",), (None,)], "x string")
    vocab = {r.value: r.idx for r in E.build_vocab(df, "x", min_count=1).collect()}
    # a and c tie on count; ties break by value. Nulls are not a vocabulary entry.
    assert vocab == {"a": 2, "c": 3, "b": 4}


def test_rare_values_are_left_out(spark):
    df = spark.createDataFrame([(1,), (1,), (2,)], "x int")
    assert [r.value for r in E.build_vocab(df, "x", min_count=2).collect()] == ["1"]


def test_unseen_rare_and_null_map_to_unk(spark):
    train = spark.createDataFrame([(10,), (10,), (11,)], "id int")
    vocab = E.build_vocab(train, "id", min_count=2)
    test = spark.createDataFrame([(10,), (11,), (99,), (None,)], "id int")
    out = {r.id: r.id_idx for r in E.apply_vocab(test, "id", vocab, "id_idx").collect()}
    assert out == {10: E.FIRST, 11: E.UNK, 99: E.UNK, None: E.UNK}


def test_vocab_from_train_only_does_not_learn_test_ids(spark):
    # The leakage rule: a vocabulary built on training rows knows nothing of
    # IDs that first appear later.
    events = spark.createDataFrame([(1, 20220410), (1, 20220411), (2, 20220505), (2, 20220506)],
                                   "user_id int, event_date int")
    train = events.filter("event_date <= 20220429")
    vocab = E.build_vocab(train, "user_id", min_count=1)
    assert [r.value for r in vocab.collect()] == ["1"]


def test_vocab_size_includes_pad_and_unk(spark):
    empty = spark.createDataFrame([], "value string, idx int, count long")
    assert E.vocab_size(empty) == 2
    df = spark.createDataFrame([("a",), ("b",)], "x string")
    assert E.vocab_size(E.build_vocab(df, "x", 1)) == 4
