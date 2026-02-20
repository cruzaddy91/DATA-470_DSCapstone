#!/usr/bin/env python
"""
Thin entry point: runs the full data pipeline via scripts/run_pipeline.py.

Usage from project root:
  python run_pipeline.py
"""
from scripts.run_pipeline import main

if __name__ == "__main__":
    main()
