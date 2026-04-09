"""
Unit Tests — Ghana MoMo ETL Pipeline
======================================
Tests all 3 pipeline stages: Extract, Transform, Load.

Run with: pytest test_pipeline.py -v

Author: Lawrence Koomson
"""

import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))
from etl_pipeline import generate_synthetic_data, transform, TRANSACTION_TYPES


# ─────────────────────────────────────────────
#  EXTRACT TESTS
# ─────────────────────────────────────────────
class TestExtract:

    def test_generates_correct_row_count(self):
        df = generate_synthetic_data(n=100)
        assert len(df) == 100

    def test_all_required_columns_present(self):
        df = generate_synthetic_data(n=50)
        required = ["transaction_id","timestamp","sender_msisdn",
                    "receiver_msisdn","amount","currency",
                    "transaction_type","operator","status","region","fee"]
        for col in required:
            assert col in df.columns, f"Missing column: {col}"

    def test_transaction_ids_are_unique(self):
        df = generate_synthetic_data(n=200)
        # Remove nulls before checking uniqueness
        valid_ids = df["transaction_id"].dropna()
        assert valid_ids.nunique() == len(valid_ids)

    def test_dirty_records_injected(self):
        """Confirms pipeline generates dirty data for cleaning demo."""
        df = generate_synthetic_data(n=1000)
        bad_amounts = df[df["amount"] == "N/A"]
        null_ids    = df[df["transaction_id"].isna()]
        assert len(bad_amounts) > 0, "Expected dirty amount records"
        assert len(null_ids) > 0,    "Expected null transaction IDs"

    def test_operators_are_valid(self):
        df  = generate_synthetic_data(n=500)
        valid_operators = {"MTN", "VODAFONE", "AIRTELTIGO"}
        actual = set(df["operator"].str.upper().unique())
        assert actual.issubset(valid_operators), f"Invalid operators: {actual - valid_operators}"


# ─────────────────────────────────────────────
#  TRANSFORM TESTS
# ─────────────────────────────────────────────
class TestTransform:

    @pytest.fixture
    def raw_data(self):
        return generate_synthetic_data(n=500)

    @pytest.fixture
    def clean_data(self, raw_data):
        df, report = transform(raw_data)
        return df, report

    def test_dirty_records_removed(self, raw_data, clean_data):
        clean_df, _ = clean_data
        assert len(clean_df) < len(raw_data)

    def test_no_null_amounts(self, clean_data):
        clean_df, _ = clean_data
        assert clean_df["amount"].isna().sum() == 0

    def test_no_null_timestamps(self, clean_data):
        clean_df, _ = clean_data
        assert clean_df["timestamp"].isna().sum() == 0

    def test_all_amounts_positive(self, clean_data):
        clean_df, _ = clean_data
        assert (clean_df["amount"] > 0).all()

    def test_time_dimensions_created(self, clean_data):
        clean_df, _ = clean_data
        for col in ["txn_date","txn_hour","txn_day_of_week","txn_month","txn_quarter"]:
            assert col in clean_df.columns, f"Missing time dimension: {col}"

    def test_amount_buckets_assigned(self, clean_data):
        clean_df, _ = clean_data
        assert "amount_bucket" in clean_df.columns
        assert clean_df["amount_bucket"].isna().sum() == 0

    def test_high_value_flag_exists(self, clean_data):
        clean_df, _ = clean_data
        assert "is_high_value" in clean_df.columns
        assert clean_df["is_high_value"].dtype == bool

    def test_rapid_succession_flag_exists(self, clean_data):
        clean_df, _ = clean_data
        assert "is_rapid_succession" in clean_df.columns

    def test_transaction_type_labels_mapped(self, clean_data):
        clean_df, _ = clean_data
        assert "transaction_type_label" in clean_df.columns
        valid_labels = set(TRANSACTION_TYPES.values()) | {"Unknown"}
        actual_labels = set(clean_df["transaction_type_label"].unique())
        assert actual_labels.issubset(valid_labels)

    def test_quality_report_keys(self, clean_data):
        _, report = clean_data
        for key in ["raw_count","clean_count","issues",
                    "high_value_flagged","success_rate_pct"]:
            assert key in report, f"Missing report key: {key}"

    def test_clean_count_less_than_raw(self, clean_data):
        _, report = clean_data
        assert report["clean_count"] < report["raw_count"]

    def test_success_rate_between_0_and_100(self, clean_data):
        _, report = clean_data
        assert 0 <= report["success_rate_pct"] <= 100

    def test_total_debit_equals_amount_plus_fee(self, clean_data):
        clean_df, _ = clean_data
        calculated = (clean_df["amount"] + clean_df["fee"]).round(2)
        assert (clean_df["total_debit"].round(2) == calculated).all()


# ─────────────────────────────────────────────
#  INTEGRATION TEST
# ─────────────────────────────────────────────
class TestIntegration:

    def test_full_pipeline_produces_output(self, tmp_path):
        """End-to-end: extract → transform → confirm output shape."""
        raw  = generate_synthetic_data(n=200)
        clean, report = transform(raw)
        assert len(clean) > 0
        assert report["clean_count"] == len(clean)
        assert report["raw_count"] == 200

    def test_no_duplicate_transaction_ids_after_transform(self):
        raw  = generate_synthetic_data(n=500)
        clean, _ = transform(raw)
        assert clean["transaction_id"].duplicated().sum() == 0

    def test_weekend_flag_is_boolean(self):
        raw  = generate_synthetic_data(n=100)
        clean, _ = transform(raw)
        assert clean["is_weekend"].dtype == bool