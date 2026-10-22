"""CTR models driven by the gold feature spec; forward(batch) returns [B] logits.

Apply sigmoid only for predictions; train with BCEWithLogitsLoss. History is
one mean-pooled field. Its embeddings are shared by DeepFM's FM and MLP,
as are the categorical and numeric field embeddings.
"""

import torch
from torch import nn


def mean_pool_history(embedding: nn.Embedding, sequence: torch.Tensor) -> torch.Tensor:
    """Exclude PAD from both sum and count; empty histories return zero."""
    mask = sequence.ne(embedding.padding_idx).unsqueeze(-1)
    values = embedding(sequence) * mask
    return values.sum(dim=1) / mask.sum(dim=1).clamp_min(1)


def fm_interactions(fields: torch.Tensor) -> torch.Tensor:
    """Sum pairwise field dot products, without materializing every pair."""
    return 0.5 * (fields.sum(dim=1).square() - fields.square().sum(dim=1)).sum(dim=1)


class LogisticRegression(nn.Module):
    """Scalar categorical/history embeddings plus a numeric linear term."""

    def __init__(self, spec: dict):
        super().__init__()
        # Do not set padding_idx on categorical tables: hour 0 is a real value.
        self.categorical = nn.ModuleList([
            nn.Embedding(c["vocab_size"], 1) for c in spec["categorical"]
        ])
        self.history = nn.Embedding(spec["sequence"]["vocab_size"], 1,
                                    padding_idx=spec["sequence"]["pad"])
        self.numeric = nn.Linear(len(spec["numeric"]) + len(spec["flags"]), 1)
        for table in [*self.categorical, self.history]:
            nn.init.zeros_(table.weight)
        nn.init.zeros_(self.numeric.weight)
        nn.init.zeros_(self.numeric.bias)

    def forward(self, batch: dict) -> torch.Tensor:
        logits = self.numeric(batch["numeric"]).squeeze(-1)
        for i, table in enumerate(self.categorical):
            logits = logits + table(batch["categorical"][:, i]).squeeze(-1)
        return logits + mean_pool_history(self.history, batch["sequence"]).squeeze(-1)


class DeepFM(nn.Module):
    """Linear + FM + MLP logits using shared field embeddings.

    Each numeric value scales its own learned vector; the pooled history is
    another field. History uses its own table, independent of target-item
    categorical embeddings, so no column names are assumed.
    """

    def __init__(self, spec: dict, embedding_dim: int = 16,
                 hidden_dims: tuple = (128, 64), dropout: float = 0.0):
        super().__init__()
        if embedding_dim < 1 or any(width < 1 for width in hidden_dims):
            raise ValueError("Embedding and hidden dimensions must be positive")
        self.linear = LogisticRegression(spec)
        self.categorical = nn.ModuleList([
            nn.Embedding(c["vocab_size"], embedding_dim) for c in spec["categorical"]
        ])
        self.history = nn.Embedding(spec["sequence"]["vocab_size"], embedding_dim,
                                    padding_idx=spec["sequence"]["pad"])
        n_numeric = len(spec["numeric"]) + len(spec["flags"])
        self.numeric_embeddings = nn.Parameter(torch.empty(n_numeric, embedding_dim))
        nn.init.normal_(self.numeric_embeddings, std=0.01)
        for table in [*self.categorical, self.history]:
            nn.init.normal_(table.weight, std=0.01)
        with torch.no_grad():
            self.history.weight[self.history.padding_idx].zero_()
        width = (len(self.categorical) + n_numeric + 1) * embedding_dim
        layers = []
        for hidden in hidden_dims:
            layers.extend([nn.Linear(width, hidden), nn.ReLU(), nn.Dropout(dropout)])
            width = hidden
        layers.append(nn.Linear(width, 1))
        self.mlp = nn.Sequential(*layers)

    def field_embeddings(self, batch: dict) -> torch.Tensor:
        fields = [table(batch["categorical"][:, i]).unsqueeze(1)
                  for i, table in enumerate(self.categorical)]
        fields.append(batch["numeric"].unsqueeze(-1) * self.numeric_embeddings)
        fields.append(mean_pool_history(self.history, batch["sequence"]).unsqueeze(1))
        return torch.cat(fields, dim=1)

    def forward(self, batch: dict) -> torch.Tensor:
        fields = self.field_embeddings(batch)
        return (self.linear(batch) + fm_interactions(fields)
                + self.mlp(fields.flatten(start_dim=1)).squeeze(-1))
