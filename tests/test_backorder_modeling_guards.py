"""Tests for split policy and notebook read-only behavior."""

from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

from src.models.backorder_modeling import (
    PreparedDataset,
    _group_split_indices,
    _recent_24_week_temporal_split_indices,
    _temporal_split_indices,
)


def _sample_prepared_dataset() -> PreparedDataset:
    features = pd.DataFrame(
        {
            "cumulative_order_quantity": [10, 12, 14, 16, 18, 20],
            "net_value": [100, 120, 140, 160, 180, 200],
            "client_id": ["100", "100", "100", "100", "100", "100"],
            "item_category": ["TAN", "TAN", "TAN", "TAN", "TAN", "TAN"],
            "sales_organization": ["1000"] * 6,
            "division": ["00"] * 6,
            "plant_code": ["1000"] * 6,
            "country_code": ["US"] * 6,
        }
    )
    target = pd.Series([0, 1, 0, 1, 0, 1], name="target_backorder_risk")
    meta = pd.DataFrame(
        {
            "client_id": ["100"] * 6,
            "sales_document_number": ["SO1", "SO1", "SO2", "SO3", "SO4", "SO5"],
            "item_number": ["10", "20", "10", "10", "10", "10"],
            "material_number": ["M1", "M2", "M3", "M4", "M5", "M6"],
            "customer_number": ["C1", "C1", "C2", "C3", "C4", "C5"],
            "order_date": pd.to_datetime(
                [
                    "2024-01-01",
                    "2024-01-02",
                    "2024-01-03",
                    "2024-01-04",
                    "2024-01-05",
                    "2024-01-06",
                ]
            ),
        }
    )
    return PreparedDataset(
        features=features,
        target=target,
        meta=meta,
        numeric_features=["cumulative_order_quantity", "net_value"],
        categorical_features=["client_id", "item_category", "sales_organization", "division", "plant_code", "country_code"],
        missing_indicator_features=[],
    )


def test_group_split_has_no_shared_sales_documents():
    dataset = _sample_prepared_dataset()
    train_index, test_index = _group_split_indices(dataset)

    train_groups = set(dataset.meta.iloc[train_index]["sales_document_number"])
    test_groups = set(dataset.meta.iloc[test_index]["sales_document_number"])
    assert train_groups.isdisjoint(test_groups)


def test_temporal_split_orders_train_before_test():
    dataset = _sample_prepared_dataset()
    train_index, test_index, metadata = _temporal_split_indices(dataset)

    max_train_date = dataset.meta.iloc[train_index]["order_date"].max()
    min_test_date = dataset.meta.iloc[test_index]["order_date"].min()
    assert max_train_date <= min_test_date
    assert metadata["split_date"] == min_test_date.strftime("%Y-%m-%d")


def test_temporal_split_expands_window_when_recent_tail_is_too_sparse():
    days = pd.date_range("2024-01-01", periods=150, freq="D")
    features = pd.DataFrame(
        {
            "cumulative_order_quantity": np.arange(150) + 1,
            "net_value": (np.arange(150) + 1) * 10,
            "client_id": ["100"] * 150,
            "item_category": ["TAN"] * 150,
            "sales_organization": ["1000"] * 150,
            "division": ["00"] * 150,
            "plant_code": ["1000"] * 150,
            "country_code": ["US"] * 150,
        }
    )
    target = pd.Series(([0] * 30) + ([1] * 60) + ([0] * 60), name="target_backorder_risk")
    meta = pd.DataFrame(
        {
            "client_id": ["100"] * 150,
            "sales_document_number": [f"SO{i:04d}" for i in range(150)],
            "item_number": ["10"] * 150,
            "material_number": [f"M{i:04d}" for i in range(150)],
            "customer_number": [f"C{i:04d}" for i in range(150)],
            "order_date": days,
        }
    )
    dataset = PreparedDataset(
        features=features,
        target=target,
        meta=meta,
        numeric_features=["cumulative_order_quantity", "net_value"],
        categorical_features=["client_id", "item_category", "sales_organization", "division", "plant_code", "country_code"],
        missing_indicator_features=[],
    )

    train_index, test_index, metadata = _temporal_split_indices(dataset)

    assert len(test_index) > 30
    assert int(dataset.target.iloc[test_index].sum()) >= 50
    assert metadata["test_window_days"] >= 90


def test_recent_24_week_split_trains_only_inside_window_and_respects_temporal_order():
    """Rows outside the last 24 weeks are excluded; train dates precede test dates within the window."""
    days = pd.date_range("2020-01-01", periods=400, freq="D")
    n = len(days)
    features = pd.DataFrame(
        {
            "cumulative_order_quantity": np.arange(n) + 1,
            "net_value": (np.arange(n) + 1) * 10,
            "client_id": ["100"] * n,
            "item_category": ["TAN"] * n,
            "sales_organization": ["1000"] * n,
            "division": ["00"] * n,
            "plant_code": ["1000"] * n,
            "country_code": ["US"] * n,
        }
    )
    target = pd.Series(([0] * 200) + ([1] * 120) + ([0] * 80), name="target_backorder_risk")
    meta = pd.DataFrame(
        {
            "client_id": ["100"] * n,
            "sales_document_number": [f"SO{i:04d}" for i in range(n)],
            "item_number": ["10"] * n,
            "material_number": [f"M{i:04d}" for i in range(n)],
            "customer_number": [f"C{i:04d}" for i in range(n)],
            "order_date": days,
        }
    )
    dataset = PreparedDataset(
        features=features,
        target=target,
        meta=meta,
        numeric_features=["cumulative_order_quantity", "net_value"],
        categorical_features=[
            "client_id",
            "item_category",
            "sales_organization",
            "division",
            "plant_code",
            "country_code",
        ],
        missing_indicator_features=[],
    )

    train_index, test_index, metadata = _recent_24_week_temporal_split_indices(dataset)

    anchor = pd.Timestamp(meta["order_date"].max())
    window_start = anchor - pd.Timedelta(weeks=24)
    train_dates = meta.iloc[train_index]["order_date"]
    test_dates = meta.iloc[test_index]["order_date"]

    assert train_dates.min() >= window_start
    assert train_dates.max() < pd.Timestamp(metadata["split_date"])
    assert test_dates.min() >= pd.Timestamp(metadata["split_date"])
    assert test_dates.max() <= anchor
    assert train_dates.max() <= test_dates.min()
    assert metadata["window_weeks"] == 24


def test_poster_figures_generate_from_saved_scores(tmp_path: Path) -> None:
    """Smoke-test ROC/PR/drift/score PNG generation (no full modeling run)."""
    os.environ.setdefault("MPLBACKEND", "Agg")
    (tmp_path / "models").mkdir()
    (tmp_path / "output" / "figures").mkdir(parents=True)
    scores = {
        "y_true": [0, 0, 1, 1],
        "baseline_positive_rate": 0.5,
        "models": {
            "logistic_regression": {
                "y_proba": [0.1, 0.2, 0.7, 0.9],
                "y_pred": [0, 0, 1, 1],
                "decision_threshold": 0.5,
                "roc_auc": 0.8,
                "pr_auc": 0.6,
            },
            "lightgbm": {
                "y_proba": [0.15, 0.25, 0.65, 0.85],
                "y_pred": [0, 0, 1, 1],
                "decision_threshold": 0.55,
                "roc_auc": 0.78,
                "pr_auc": 0.55,
            },
        },
    }
    (tmp_path / "models" / "temporal_holdout_test_scores_v2_ordertime.json").write_text(
        json.dumps(scores), encoding="utf-8"
    )
    diag = {"target_stability": {"monthly_tail": [{"order_month": "2024-01", "positive_rate": 0.05}]}}
    (tmp_path / "models" / "auc_diagnostics_v2_ordertime.json").write_text(json.dumps(diag), encoding="utf-8")

    from src.models.poster_figures_v2 import generate_temporal_holdout_poster_figures

    out = generate_temporal_holdout_poster_figures(tmp_path)
    assert Path(out["roc"]).exists()
    assert Path(out["pr"]).exists()
    assert Path(out["drift"]).exists()
    assert Path(out["scores"]).exists()


def test_modeling_notebook_is_read_only():
    notebook = Path("notebooks/02_modeling.ipynb")
    data = json.loads(notebook.read_text())
    source = "\n".join("".join(cell.get("source", [])) for cell in data["cells"])

    forbidden_snippets = [
        "joblib.dump(",
        "train_test_split(",
        "Ridge(",
        "ElasticNet(",
        "log1p(backorder_units)",
        "backorder_ridge",
        "backorder_elasticnet",
        "json.dump(",
    ]
    for snippet in forbidden_snippets:
        assert snippet not in source
