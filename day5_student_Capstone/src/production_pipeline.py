from pathlib import Path

import numpy as np
import pandas as pd

EXPECTED_COLUMNS = [
    "well_id",
    "timestamp",
    "oil_bbl",
    "water_bbl",
    "gas_mscf",
    "pressure_psi",
    "status",
    "ingested_at",
]
MEASUREMENTS = ["oil_bbl", "water_bbl", "gas_mscf", "pressure_psi"]
PRODUCTION = ["oil_bbl", "water_bbl", "gas_mscf"]

#parse timestamp using the supplied YY-MM_DD HH:MM
"""Parse either timestamp format used in the brief and supplied table."""
def _parse_datetime_column(series):

    text = series.astype("string").str.strip()
    parsed = pd.to_datetime(text, format="%Y-%m-%d %H:%M", errors="coerce")

    # The supplied table uses dates such as 4/6/2026 18:00.
    alternate = pd.to_datetime(text, format="%m/%d/%Y %H:%M", errors="coerce")
    return parsed.fillna(alternate)

def ingest_and_validate(path):
    """Read the CSV and separate valid timestamps from rejected rows."""
    raw = pd.read_csv(path)

    if list(raw.columns) != EXPECTED_COLUMNS:
        raise ValueError(
            f"Expected these columns, in this order: {EXPECTED_COLUMNS}. "
            f"Found: {list(raw.columns)}"
        )

    raw = raw.copy()
    raw["source_row_number"] = np.arange(2, len(raw) + 2)
    raw["timestamp_raw"] = raw["timestamp"]
    raw["ingested_at_raw"] = raw["ingested_at"]

    raw["timestamp"] = _parse_datetime_column(raw["timestamp"])
    raw["ingested_at"] = _parse_datetime_column(raw["ingested_at"])

    invalid_timestamp = raw["timestamp"].isna()
    rejected = raw.loc[invalid_timestamp].copy()
    rejected["rejection_reason"] = "Timestamp could not be parsed"
    accepted = raw.loc[~invalid_timestamp].copy()

    return accepted, rejected

#Deduplicate by (well_id, timstamp) keeping the row with the latest valid ingested at timestamp
def repair_and_align(valid_rows):
    """Deduplicate, clean measurements, and build a six-hour grid per well."""
    if valid_rows.empty:
        raise ValueError("There are no rows with valid timestamps.")

    df = valid_rows.copy()

    # Keep the row with the most recent valid ingestion time.
    # Stable sorting makes ties deterministic: the later source row wins.
    df["_source_order"] = np.arange(len(df))
    df["_ingestion_sort"] = df["ingested_at"].fillna(
        pd.Timestamp.min
    )
    df = df.sort_values(
        ["_ingestion_sort", "_source_order"], kind="stable"
    )
    before_deduplication = len(df)
    df = df.drop_duplicates(
        subset=["well_id", "timestamp"], keep="last"
    )
    duplicates_removed = before_deduplication - len(df)

    # Convert invalid and negative measurements to NaN; keep real zeroes.
    for column in MEASUREMENTS:
        df[column] = pd.to_numeric(df[column], errors="coerce")
        df.loc[df[column] < 0, column] = np.nan

    # Preserve original missingness for audit, including invalid measurements.
    for column in MEASUREMENTS:
        df[f"{column}_was_missing"] = df[column].isna()

    aligned_wells = []
    inserted_slots = 0

    for well_id, well in df.groupby("well_id", sort=True):
        well = well.sort_values("timestamp").set_index("timestamp")

        grid = pd.date_range(
            start=well.index.min(),
            end=well.index.max(),
            freq="6h",
        )

        original_times = well.index
        aligned = well.reindex(grid)
        aligned.index.name = "timestamp"
        aligned["well_id"] = well_id
        aligned["is_inserted_slot"] = ~aligned.index.isin(original_times)
        inserted_slots += int(aligned["is_inserted_slot"].sum())

        # An inserted slot has no source measurements.
        for column in MEASUREMENTS:
            flag = f"{column}_was_missing"
            aligned[flag] = aligned[flag].fillna(True).astype(bool)

        aligned_wells.append(aligned.reset_index())

    cleaned = pd.concat(aligned_wells, ignore_index=True)
    cleaned = cleaned.sort_values(["well_id", "timestamp"]).reset_index(drop=True)

    # Fill only a single internal pressure gap. No edge extrapolation.
    for _, indexes in cleaned.groupby("well_id").groups.items():
        pressure = cleaned.loc[indexes, "pressure_psi"]
        interpolated = pressure.interpolate(
            method="linear",
            limit=1,
            limit_area="inside",
        )
        cleaned.loc[indexes, "pressure_psi"] = interpolated

    # Derived values require both inputs; water cut also requires a positive sum.
    has_oil_and_water = cleaned["oil_bbl"].notna() & cleaned["water_bbl"].notna()
    cleaned["liquid_bbl"] = np.where(
        has_oil_and_water,
        cleaned["oil_bbl"] + cleaned["water_bbl"],
        np.nan,
    )

    denominator = cleaned["oil_bbl"] + cleaned["water_bbl"]
    can_calculate_cut = has_oil_and_water & denominator.gt(0)
    cleaned["water_cut_pct"] = np.where(
        can_calculate_cut,
        cleaned["water_bbl"] / denominator * 100,
        np.nan,
    )

    report_rows = [
        {"metric": "rows_after_timestamp_validation", "count": len(valid_rows)},
        {"metric": "duplicate_rows_removed", "count": duplicates_removed},
        {"metric": "inserted_six_hour_slots", "count": inserted_slots},
    ]

    for column in MEASUREMENTS:
        report_rows.append(
            {
                "metric": f"missing_{column}_after_cleaning",
                "count": int(cleaned[column].isna().sum()),
            }
        )

    report = pd.DataFrame(report_rows)
    return cleaned, report


def summarize_daily(cleaned_rows):
    """Create daily well summaries, counts, and completeness flags."""
    df = cleaned_rows.copy()
    df["date"] = pd.to_datetime(df["timestamp"]).dt.normalize()

    grouped = df.groupby(["well_id", "date"], as_index=False)

    daily = grouped.agg(
        expected_slots=("timestamp", "size"),
        valid_oil_readings=("oil_bbl", "count"),
        valid_water_readings=("water_bbl", "count"),
        valid_gas_readings=("gas_mscf", "count"),
    )

    # min_count=1 preserves NaN for a day with no valid readings.
    totals = (
        df.groupby(["well_id", "date"])[PRODUCTION]
        .sum(min_count=1)
        .reset_index()
    )
    daily = daily.merge(totals, on=["well_id", "date"], how="left")

    for measurement in PRODUCTION:
        count_column = f"valid_{measurement.removesuffix('_bbl').removesuffix('_mscf')}_readings"
        daily[f"{measurement}_complete"] = (
            daily[count_column] == daily["expected_slots"]
        )

    return daily.sort_values(["well_id", "date"]).reset_index(drop=True)


def run_pipeline(input_csv, output_dir):
    """Run the pipeline and write all four required CSV files."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    accepted, rejected = ingest_and_validate(input_csv)
    cleaned, report = repair_and_align(accepted)
    daily = summarize_daily(cleaned)

    cleaned.to_csv(output_dir / "cleaned_production.csv", index=False)
    report.to_csv(output_dir / "data_quality_report.csv", index=False)
    daily.to_csv(output_dir / "daily_production_summary.csv", index=False)
    rejected.to_csv(output_dir / "rejected_source_records.csv", index=False)