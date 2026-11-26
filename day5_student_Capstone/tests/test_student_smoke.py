from pathlib import Path

import numpy as np
import pandas as pd

from src.production_pipeline import (
    EXPECTED_COLUMNS,
    ingest_and_validate,
    repair_and_align,
    summarize_daily,
)


DATA = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "historical_production_capstone.csv"
)


def test_source_columns():
    """The CSV has the eight required columns, in the required order."""
    actual_columns = list(pd.read_csv(DATA, nrows=0).columns)
    assert actual_columns == EXPECTED_COLUMNS


def test_ingestion_separates_invalid_timestamp():
    """The malformed timestamp is rejected with an explanatory reason."""
    accepted, rejected = ingest_and_validate(DATA)

    assert len(accepted) == 46
    assert len(rejected) == 1
    assert "rejection_reason" in rejected.columns
    assert rejected["rejection_reason"].notna().all()


def test_duplicate_removal_and_output_shapes():
    """Deduplication and alignment produce the expected row counts."""
    accepted, _ = ingest_and_validate(DATA)
    cleaned, report = repair_and_align(accepted)
    daily = summarize_daily(cleaned)

    assert len(cleaned) == 48
    assert len(daily) == 12
    assert isinstance(report, pd.DataFrame)

    duplicate_count = report.loc[
        report["metric"] == "duplicate_rows_removed", "count"
    ].item()
    assert duplicate_count == 2

    inserted_count = report.loc[
        report["metric"] == "inserted_six_hour_slots", "count"
    ].item()
    assert inserted_count == 4


def test_genuine_zero_readings_are_kept():
    """A shut-in zero is a real reading, not a missing value."""
    accepted, _ = ingest_and_validate(DATA)
    cleaned, _ = repair_and_align(accepted)

    shut_in = cleaned.loc[
        (cleaned["well_id"] == "WELL-22")
        & (cleaned["timestamp"] == pd.Timestamp("2026-04-07 18:00"))
    ].iloc[0]

    assert shut_in["oil_bbl"] == 0
    assert shut_in["water_bbl"] == 0
    assert shut_in["gas_mscf"] == 0


def test_negative_measurement_becomes_missing():
    """The negative water reading is at 06:00 and must become NaN."""
    accepted, _ = ingest_and_validate(DATA)
    cleaned, _ = repair_and_align(accepted)

    row = cleaned.loc[
        (cleaned["well_id"] == "WELL-21")
        & (cleaned["timestamp"] == pd.Timestamp("2026-04-08 06:00"))
    ].iloc[0]

    assert np.isnan(row["water_bbl"])


def test_invalid_numeric_measurement_becomes_missing():
    """Nonnumeric gas data becomes NaN rather than zero."""
    accepted, _ = ingest_and_validate(DATA)
    cleaned, _ = repair_and_align(accepted)

    row = cleaned.loc[
        (cleaned["well_id"] == "WELL-23")
        & (cleaned["timestamp"] == pd.Timestamp("2026-04-08 00:00"))
    ].iloc[0]

    assert np.isnan(row["gas_mscf"])