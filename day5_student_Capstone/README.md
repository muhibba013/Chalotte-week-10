README.md
# Historical Production Data Quality Pipeline
The smoke checks cover the input schema, invalid timestamp handling, expected aligned and daily row counts, duplicate and inserted-slot counts, retention of genuine zeros, and conversion of negative or nonnumeric readings to missing values.

Run the notebook

Start Jupyter from the project root:


python -m jupyter notebook


Open notebook/Week10_Class5_Student_Capstone.ipynb and run all cells from top to bottom. The notebook imports and demonstrates the functions in src/production_pipeline.py, displays quality and daily-summary results, and compares actual counts with the brief's acceptance targets.

The provided table uses timestamps such as 4/6/2026 18:00 (M/D/YYYY H:MM), while the brief describes YYYY-MM-DD HH:MM. The ingestion parser should document and support the format present in the supplied CSV; malformed timestamps are rejected rather than silently dropped.

Pipeline decisions and limitations

- Duplicate records are grouped by (well_id, timestamp); the latest valid ingested_at is retained.
- Each well receives its own six-hour grid from its earliest through latest valid timestamp. Inserted slots indicate missing observations, not zero production.
- Nonnumeric and negative oil, water, gas, and pressure readings are set to missing. Zero is a valid measured value.
- Oil, water, and gas are never interpolated. Pressure interpolation is restricted to a single internal gap.
- Daily completeness is reported alongside totals because totals based on partial readings may understate production.
- The dataset is synthetic and small. Results and operational observations should not be treated as real field guidance.

Pull request


