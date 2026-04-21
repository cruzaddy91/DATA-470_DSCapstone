"""Guardrails for the leakage-safe order-time backorder workflow."""

from src.features.build_targets import (
    ORDERTIME_FORBIDDEN_COLUMNS,
    ORDERTIME_MODELING_TABLE,
    ORDERTIME_NUMERIC_FEATURES,
    ORDERTIME_CATEGORICAL_FEATURES,
)
from src.models.backorder_modeling import (
    ARTIFACT_SUFFIX,
    LEAKY_COLUMNS,
    MODEL_FILE_MAP,
    MODELING_TABLE_FILE,
    RAW_CATEGORICAL_FEATURES,
    RAW_NUMERIC_FEATURES,
)


def test_order_time_safe_features_are_disjoint_from_leaky_columns():
    safe_features = set(RAW_NUMERIC_FEATURES) | set(RAW_CATEGORICAL_FEATURES)
    assert safe_features.isdisjoint(LEAKY_COLUMNS)


def test_known_proxy_columns_are_excluded_from_v2_features():
    safe_features = set(ORDERTIME_NUMERIC_FEATURES) | set(ORDERTIME_CATEGORICAL_FEATURES)
    forbidden = {"material_type", "distribution_channel", "order_header_net_value", "total_requested_quantity"}
    assert safe_features.isdisjoint(forbidden)


def test_modeling_table_name_is_versioned():
    assert ORDERTIME_MODELING_TABLE.endswith("v2_ordertime")
    assert MODELING_TABLE_FILE == f"{ORDERTIME_MODELING_TABLE}.csv"


def test_artifact_names_use_ordertime_suffix():
    for artifact_name in MODEL_FILE_MAP.values():
        assert artifact_name.endswith(f"{ARTIFACT_SUFFIX}.joblib")


def test_build_target_forbidden_columns_match_model_leak_list():
    assert set(ORDERTIME_FORBIDDEN_COLUMNS) == set(LEAKY_COLUMNS)
