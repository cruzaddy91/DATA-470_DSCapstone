#!/usr/bin/env python
"""
Run the full data pipeline: core master tables + BRD metrics.

Usage from project root:
  python run_pipeline.py

  Or:
  python -m src.data.run_pipeline
"""
from src.data.run_pipeline import main

if __name__ == "__main__":
    main()
