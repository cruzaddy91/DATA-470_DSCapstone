"""Shared column preprocessing for v2 order-time tabular models (LR and LightGBM)."""

from __future__ import annotations

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


def build_v2_column_preprocessor(
    numeric_features: list[str],
    categorical_features: list[str],
) -> ColumnTransformer:
    """
    Median imputation + one-hot encoding with unknown category handling.

    Used identically by logistic regression and gradient boosting pipelines so
    feature space stays comparable across models.
    """
    return ColumnTransformer(
        transformers=[
            (
                "num",
                Pipeline(steps=[("impute", SimpleImputer(strategy="median"))]),
                numeric_features,
            ),
            (
                "cat",
                Pipeline(
                    steps=[
                        ("impute", SimpleImputer(strategy="constant", fill_value="Missing")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                categorical_features,
            ),
        ],
        remainder="drop",
    )
