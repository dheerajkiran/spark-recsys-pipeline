"""Model math, padding, gradients, and small-batch learning checks."""

import pytest
import torch
from torch import nn

from training.models import DeepFM, LogisticRegression, fm_interactions, mean_pool_history


SPEC = {
    "categorical": [{"name": "category", "vocab_size": 32}],
    "numeric": ["value"], "flags": ["missing"],
    "sequence": {"name": "history", "vocab_size": 40, "pad": 0, "length": 4},
}


def batch():
    return {
        "categorical": torch.arange(16).unsqueeze(1),
        "numeric": torch.zeros(16, 2),
        "sequence": torch.tensor([[0, 0, 2, 3]] * 16),
        "label": (torch.arange(16) % 2).float(),
    }


@pytest.mark.parametrize("model_type", [LogisticRegression, DeepFM])
@pytest.mark.parametrize("size", [1, 16])
def test_output_and_gradients(model_type, size):
    torch.manual_seed(42)
    model = model_type(SPEC)
    b = {key: value[:size] for key, value in batch().items()}
    b["numeric"].fill_(1)
    logits = model(b)
    assert logits.shape == (size,)
    assert torch.isfinite(logits).all()
    nn.BCEWithLogitsLoss()(logits, b["label"]).backward()
    assert all(p.grad is not None and torch.isfinite(p.grad).all() for p in model.parameters())
    if isinstance(model, DeepFM):
        assert model.numeric_embeddings.grad.abs().sum() > 0
        assert model.history.weight.grad[2:].abs().sum() > 0
    # Categorical zero is valid and must learn; PAD history must not learn.
    assert model.categorical[0].weight.grad[0].abs().sum() > 0
    assert model.history.weight.grad[0].count_nonzero() == 0


def test_history_mean_excludes_padding_even_with_nonzero_pad_embedding():
    table = nn.Embedding(5, 2, padding_idx=0)
    with torch.no_grad():
        table.weight.copy_(torch.tensor([[99., 99.], [2., 4.], [6., 8.], [0., 0.], [0., 0.]]))
    actual = mean_pool_history(table, torch.tensor([[0, 1, 0, 2], [0, 0, 0, 0]]))
    torch.testing.assert_close(actual, torch.tensor([[4., 6.], [0., 0.]]))


@pytest.mark.parametrize("model_type", [LogisticRegression, DeepFM])
def test_extra_padding_does_not_change_predictions(model_type):
    torch.manual_seed(42)
    model = model_type(SPEC).eval()
    # LR starts at zero; use nonzero history weights so this check is meaningful.
    with torch.no_grad():
        model.history.weight.normal_()
    b = batch()
    expected = model(b)
    b["sequence"] = torch.tensor([[3, 0, 2, 0, 0, 0]] * 16)
    torch.testing.assert_close(model(b), expected)
    b["sequence"].zero_()
    assert torch.isfinite(model(b)).all()


def test_lr_matches_manual_linear_score():
    model = LogisticRegression(SPEC)
    with torch.no_grad():
        model.categorical[0].weight.copy_(torch.arange(32).float().unsqueeze(1))
        model.numeric.weight.copy_(torch.tensor([[2., -3.]]))
        model.numeric.bias.fill_(0.5)
        model.history.weight.copy_(torch.arange(40).float().unsqueeze(1))
    b = batch()
    b["numeric"][:, 0] = 4
    b["numeric"][:, 1] = 1
    # category weight + (2*4 - 3*1) + bias + mean(history weights 2, 3).
    torch.testing.assert_close(model(b), torch.arange(16).float() + 8.)


def test_fm_matches_explicit_pairwise_dot_products():
    torch.manual_seed(7)
    fields = torch.randn(3, 5, 4, dtype=torch.float64, requires_grad=True)
    expected = sum((fields[:, i] * fields[:, j]).sum(dim=1)
                   for i in range(5) for j in range(i + 1, 5))
    actual = fm_interactions(fields)
    torch.testing.assert_close(actual, expected)
    torch.testing.assert_close(torch.autograd.grad(actual.sum(), fields, retain_graph=True)[0],
                               torch.autograd.grad(expected.sum(), fields)[0])


def test_deepfm_matches_manual_full_score():
    spec = {**SPEC, "categorical": [{"name": "category", "vocab_size": 3}],
            "sequence": {**SPEC["sequence"], "vocab_size": 4}}
    model = DeepFM(spec, embedding_dim=2, hidden_dims=(2,)).double().eval()
    linear_cat, linear_numeric = [0.2, -0.1, 0.4], [0.6, -0.3]
    linear_history, linear_bias = [9., 0.1, 0.3, -0.2], 0.05
    cat_vectors = [[0.2, 0.5], [-0.4, 0.3], [0.1, -0.2]]
    numeric_vectors = [[0.3, -0.2], [0.4, 0.1]]
    history_vectors = [[8., 9.], [0.1, -0.3], [0.4, 0.2], [-0.2, 0.6]]
    hidden_weights = [[0.05 * (i + 1) for i in range(8)],
                      [-0.04 * (i + 1) for i in range(8)]]
    hidden_bias, output_weights, output_bias = [-0.1, 0.2], [0.7, -0.4], 0.03
    with torch.no_grad():
        for parameter, values in [
            (model.linear.categorical[0].weight, [[x] for x in linear_cat]),
            (model.linear.numeric.weight, [linear_numeric]),
            (model.linear.numeric.bias, [linear_bias]),
            (model.linear.history.weight, [[x] for x in linear_history]),
            (model.categorical[0].weight, cat_vectors),
            (model.numeric_embeddings, numeric_vectors),
            (model.history.weight, history_vectors),
            (model.mlp[0].weight, hidden_weights),
            (model.mlp[0].bias, hidden_bias),
            (model.mlp[3].weight, [output_weights]),
            (model.mlp[3].bias, [output_bias]),
        ]:
            parameter.copy_(torch.tensor(values, dtype=torch.float64))

    categories = [0, 1, 2]
    numerics = [[0.4, 1.], [-0.7, 0.], [0., 1.]]
    sequences = [[0, 2, 0, 3], [0, 0, 0, 0], [1, 1, 0, 0]]
    b = {"categorical": torch.tensor(categories).unsqueeze(1),
         "numeric": torch.tensor(numerics, dtype=torch.float64),
         "sequence": torch.tensor(sequences)}
    expected = []
    for category, values, sequence in zip(categories, numerics, sequences):
        history = [index for index in sequence if index != spec["sequence"]["pad"]]
        count = max(1, len(history))
        linear = (linear_bias + linear_cat[category]
                  + sum(w * x for w, x in zip(linear_numeric, values))
                  + sum(linear_history[index] for index in history) / count)
        fields = [cat_vectors[category]]
        fields += [[x * v for v in vector] for x, vector in zip(values, numeric_vectors)]
        fields += [[sum(history_vectors[index][k] for index in history) / count
                    for k in range(2)]]
        fm = sum(sum(x * y for x, y in zip(fields[i], fields[j]))
                 for i in range(len(fields)) for j in range(i + 1, len(fields)))
        flat = [x for field in fields for x in field]
        hidden = [max(0., sum(w * x for w, x in zip(weights, flat)) + bias)
                  for weights, bias in zip(hidden_weights, hidden_bias)]
        mlp = sum(w * x for w, x in zip(output_weights, hidden)) + output_bias
        expected.append(linear + fm + mlp)
    torch.testing.assert_close(model(b), torch.tensor(expected, dtype=torch.float64),
                               rtol=1e-12, atol=1e-12)


@pytest.mark.parametrize("model_type", [LogisticRegression, DeepFM])
def test_overfit_one_small_batch(model_type):
    torch.manual_seed(42)
    model = model_type(SPEC)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.05)
    b = batch()
    loss_fn = nn.BCEWithLogitsLoss()
    for _ in range(200):
        optimizer.zero_grad()
        loss = loss_fn(model(b), b["label"])
        loss.backward()
        optimizer.step()
    with torch.no_grad():
        logits = model(b)
        assert loss_fn(logits, b["label"]).item() < 0.02
        assert torch.equal((logits > 0).float(), b["label"])
