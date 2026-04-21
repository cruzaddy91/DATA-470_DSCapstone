"""Tests for the versioned order-time backorder data contract."""

from __future__ import annotations

import pandas as pd
import pytest

from src.features.build_targets import (
    SNAPSHOT_BACKORDER_INT_TABLE,
    SNAPSHOT_BACKORDER_MODELING_TABLE,
    SNAPSHOT_BACKORDER_STG_TABLE,
    SNAPSHOT_BACKORDER_TARGET_COLUMN,
    SNAPSHOT_BACKORDER_UNITS_COLUMN,
    SNAPSHOT_OSQ_COLUMN,
    SNAPSHOT_RULE_FLAG_COLUMN,
    SNAPSHOT_SI_COLUMN,
    DEMAND_INVENTORY_NUMERIC_FEATURES,
    ORDER_GRAIN_COLUMNS,
    TARGET_OBSERVED_COLUMN,
    TARGET_STATUS_COLUMN,
    TARGET_COLUMN,
    build_ordertime_demand_inventory_24wk_dataset,
    build_ordertime_modeling_dataset,
    build_snapshot_backorder_bundle,
    build_snapshot_backorder_modeling_dataset,
)
from src.models.backorder_modeling import prepare_backorder_dataset, prepare_snapshot_backorder_dataset


def _sample_master_order() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "client_id": "100",
                "sales_document_number": "5001",
                "item_number": "10",
                "material_number": "MAT-1",
                "customer_number": "CUST-1",
                "order_date": "2024-01-01",
                "requested_delivery_date": "2024-01-05",
                "requested_delivery_date_schedule": pd.NA,
                "cumulative_order_quantity": 10,
                "cumulative_confirmed_quantity": 9,
                "net_value": 120.0,
                "item_category": "TAN",
                "sales_organization": "1000",
                "division": "00",
                "plant_code": "1000",
                "country_code": "US",
            },
            {
                "client_id": "100",
                "sales_document_number": "5002",
                "item_number": "20",
                "material_number": "MAT-2",
                "customer_number": "CUST-2",
                "order_date": "2024-01-02",
                "requested_delivery_date": "2024-01-06",
                "requested_delivery_date_schedule": "2024-01-07",
                "cumulative_order_quantity": 15,
                "cumulative_confirmed_quantity": pd.NA,
                "net_value": pd.NA,
                "item_category": pd.NA,
                "sales_organization": "2000",
                "division": pd.NA,
                "plant_code": "2000",
                "country_code": "CA",
            },
            {
                "client_id": "100",
                "sales_document_number": "5003",
                "item_number": "30",
                "material_number": "MAT-3",
                "customer_number": "CUST-3",
                "order_date": "2024-03-15",
                "requested_delivery_date": "2024-03-20",
                "requested_delivery_date_schedule": pd.NA,
                "cumulative_order_quantity": 5,
                "cumulative_confirmed_quantity": 5,
                "net_value": 40.0,
                "item_category": "TAN",
                "sales_organization": "1000",
                "division": "00",
                "plant_code": "1000",
                "country_code": "US",
            },
        ]
    )


def _sample_master_order_brd() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "client_id": "100",
                "sales_document_number": "5001",
                "item_number": "10",
                "outstanding_qty": 0.0,
                "saleable_inventory": 100.0,
                "backorder_units": 0,
                "total_quantity_delivered": 10,
                "delivery_status": "C",
                "order_date": "2024-01-01",
                "requested_delivery_date": "2024-01-05",
                "requested_delivery_date_schedule": pd.NA,
                "cumulative_order_quantity": 10,
            },
            {
                "client_id": "100",
                "sales_document_number": "5002",
                "item_number": "20",
                "outstanding_qty": 10.0,
                "saleable_inventory": 3.0,
                "backorder_units": 3,
                "total_quantity_delivered": 12,
                "delivery_status": "B",
                "order_date": "2024-01-02",
                "requested_delivery_date": "2024-01-06",
                "requested_delivery_date_schedule": "2024-01-07",
                "cumulative_order_quantity": 15,
            },
            {
                "client_id": "100",
                "sales_document_number": "5003",
                "item_number": "30",
                "outstanding_qty": 4.0,
                "saleable_inventory": 4.0,
                "backorder_units": 5,
                "total_quantity_delivered": pd.NA,
                "delivery_status": "A",
                "order_date": "2024-03-15",
                "requested_delivery_date": "2024-03-20",
                "requested_delivery_date_schedule": pd.NA,
                "cumulative_order_quantity": 5,
            },
        ]
    )


def test_build_snapshot_backorder_modeling_dataset_shortfall_formula():
    order = _sample_master_order()
    brd = _sample_master_order_brd()
    mart = build_snapshot_backorder_modeling_dataset(order, brd)

    assert len(mart) == 3
    assert mart[SNAPSHOT_BACKORDER_TARGET_COLUMN].tolist() == [0, 1, 0]
    assert mart[SNAPSHOT_BACKORDER_UNITS_COLUMN].tolist() == [0.0, 7.0, 0.0]
    assert SNAPSHOT_OSQ_COLUMN in mart.columns
    assert SNAPSHOT_SI_COLUMN in mart.columns


def test_build_v3_demand_inventory_adds_trailing_shipments_and_woc():
    order = _sample_master_order()
    brd = _sample_master_order_brd()
    v2 = build_ordertime_modeling_dataset(order, brd)
    shipment = pd.DataFrame(
        [
            {
                "client_id": "100",
                "material_number": "MAT-1",
                "plant_code": "1000",
                "shipment_week": "2023-12-25",
                "quantity_shipped": 10.0,
            },
        ]
    )
    woc = pd.DataFrame(
        [
            {
                "client_id": "100",
                "material_number": "MAT-1",
                "plant_code": "1000",
                "saleable_inventory": 50.0,
                "awd": 2.0,
                "woc": 5.0,
            },
        ]
    )
    v3 = build_ordertime_demand_inventory_24wk_dataset(v2, shipment, woc)
    for column in DEMAND_INVENTORY_NUMERIC_FEATURES:
        assert column in v3.columns
    row = v3.loc[v3["item_number"].astype(str) == "10"].iloc[0]
    assert float(row["demand_shipped_24wk_sum"]) == 10.0
    assert float(row["inventory_saleable_si"]) == 50.0


def test_build_snapshot_backorder_bundle_layers_align():
    order = _sample_master_order()
    brd = _sample_master_order_brd()
    bundle = build_snapshot_backorder_bundle(order, brd)

    stg = bundle[SNAPSHOT_BACKORDER_STG_TABLE]
    int_df = bundle[SNAPSHOT_BACKORDER_INT_TABLE]
    mart = bundle[SNAPSHOT_BACKORDER_MODELING_TABLE]

    assert len(stg) == len(int_df) == len(mart) == 3
    assert set(stg.columns) >= {SNAPSHOT_OSQ_COLUMN, SNAPSHOT_SI_COLUMN, "material_number"}
    assert int_df[SNAPSHOT_RULE_FLAG_COLUMN].tolist() == [0, 1, 0]
    assert int_df[SNAPSHOT_RULE_FLAG_COLUMN].tolist() == int_df[SNAPSHOT_BACKORDER_TARGET_COLUMN].tolist()
    merged_check = stg.merge(int_df, on=ORDER_GRAIN_COLUMNS, how="inner", validate="one_to_one")
    assert len(merged_check) == 3


def test_prepare_snapshot_backorder_round_trip(tmp_path):
    order = _sample_master_order()
    brd = _sample_master_order_brd()
    mart = build_snapshot_backorder_modeling_dataset(order, brd)
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)
    mart.to_csv(processed / f"{SNAPSHOT_BACKORDER_MODELING_TABLE}.csv", index=False)

    prepared = prepare_snapshot_backorder_dataset(tmp_path)
    assert prepared.target.tolist() == [0, 1, 0]
    assert SNAPSHOT_OSQ_COLUMN not in prepared.features.columns
    assert SNAPSHOT_SI_COLUMN not in prepared.features.columns
    assert prepared.osq_si_label_inputs is not None
    assert prepared.osq_si_label_inputs.columns.tolist() == [
        SNAPSHOT_OSQ_COLUMN,
        SNAPSHOT_SI_COLUMN,
    ]


def test_build_ordertime_modeling_dataset_keeps_only_order_time_columns():
    dataset = build_ordertime_modeling_dataset(_sample_master_order(), _sample_master_order_brd())

    assert list(dataset[ORDER_GRAIN_COLUMNS].columns) == ORDER_GRAIN_COLUMNS
    assert TARGET_COLUMN in dataset.columns
    assert TARGET_OBSERVED_COLUMN in dataset.columns
    assert TARGET_STATUS_COLUMN in dataset.columns
    assert "backorder_units" not in dataset.columns
    assert "total_quantity_delivered" not in dataset.columns
    assert "distribution_channel" not in dataset.columns
    assert dataset[TARGET_COLUMN].iloc[0] == 0
    assert dataset[TARGET_COLUMN].iloc[1] == 1
    assert pd.isna(dataset[TARGET_COLUMN].iloc[2])
    assert dataset[TARGET_OBSERVED_COLUMN].tolist() == [1, 1, 0]
    assert dataset[TARGET_STATUS_COLUMN].tolist() == ["negative", "positive", "pending_window"]


def test_build_ordertime_modeling_dataset_rejects_duplicate_grain():
    master_order = pd.concat([_sample_master_order(), _sample_master_order().iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="violates the order grain"):
        build_ordertime_modeling_dataset(master_order, _sample_master_order_brd())


def test_prepare_backorder_dataset_adds_missing_indicators(tmp_path):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)
    dataset = build_ordertime_modeling_dataset(_sample_master_order(), _sample_master_order_brd())
    dataset.to_csv(processed / "master_order_fulfillment_modeling_v2_ordertime.csv", index=False)

    prepared = prepare_backorder_dataset(tmp_path)

    assert "missing__net_value" in prepared.features.columns
    assert "missing__item_category" in prepared.features.columns
    assert "missing__net_value" in prepared.numeric_features
    assert "missing__item_category" in prepared.numeric_features
    assert prepared.target.tolist() == [0, 1]
    assert len(prepared.features) == 2
