"""
Run the full data pipeline: core master tables + BRD metrics.

Usage:
  python -m src.data.run_pipeline

  Or from project root:
  python run_pipeline.py  (if run_pipeline.py exists at root)

Pipeline:
  1. build_master_tables  -> master_order_fulfillment, master_inventory_material, master_purchase
  2. build_brd_metrics    -> master_order_fulfillment_brd, shipment_history, master_woc
  3. build_targets        -> master_order_fulfillment_with_targets, master_inventory_material_with_targets
"""

import sys
from pathlib import Path

# Ensure project root is on path
_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))


def main():
    from src.data.build_master_tables import build_all_master_tables
    from src.data.build_brd_metrics import build_all_brd_metrics
    from src.features.build_targets import build_all_targets

    print("=" * 60)
    print("Pipeline Step 1: Core master tables")
    print("=" * 60)
    build_all_master_tables(project_root=_project_root)

    print()
    print("=" * 60)
    print("Pipeline Step 2: BRD metrics")
    print("=" * 60)
    build_all_brd_metrics(project_root=_project_root)

    print()
    print("=" * 60)
    print("Pipeline Step 3: ML targets")
    print("=" * 60)
    build_all_targets(project_root=_project_root)

    print()
    print("=" * 60)
    print("Pipeline complete. Outputs in data/processed/")
    print("Next: run 02_modeling.ipynb or python scripts/run_modeling.py for ML.")
    print("=" * 60)


if __name__ == "__main__":
    main()
