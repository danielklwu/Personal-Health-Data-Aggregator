# Personal-Health-Data-Aggregator

## Project Goal
Health data is notoriously fragmented. This project merges two datasets that don't perfectly align:
- Sleep logs (timestamps in UTC)
- Workout logs (timestamps in local time)

### Objectives
- Normalize timestamps across sources
- Attribute each event to the correct UTC day
- Aggregate into daily summaries
- Compute a simple metric (average calories burned on days below a threshold)

## Breaking Down the Task
1. **Ingestion:** Load `sleep.json` and `workouts.json`
2. **Normalization:** Parse timestamps, normalize units, harmonize timezones
3. **Alignment:** Attribute events to the correct day, handling day boundaries
4. **Aggregation:** Merge and summarize by day
5. **Metric Calculation:** Compute average calories burned on days with low sleep
6. **Validation:** Test all logic and edge cases

## Methodology

### Tool Choices & Reasoning
- **Timestamp Conversion:**
  - Considered using manual math, `pytz`, and `zoneinfo`. Decided on `zoneinfo` (Python 3.9+) over `pytz` for timezone conversion because it's built-in, standards-compliant, and avoids external dependencies. This ensures the correct handling of daylight saving and ambiguous times.
- **Data Parsing:**
  - Used Python's `json` and `datetime` modules for robust, readable parsing.
- **Aggregation & Metrics:**
  - Designed modular functions for merging and metric calculation, making it easy to extend and test.
- **Testing:**
  - Wrote comprehensive unit tests for normalization, merging, and metrics. Edge cases (such as day boundary crossings) are explicitly tested.
- **AI Usage Disclosure:**
  - AI was used to generate input JSON files and unit test example data. Unit test results were manually verified to ensure accuracy and reliability.

### Output Verification
- **Unit Tests:**
  - Created 23+ tests covering timestamp parsing, timezone conversion, merging, and metric calculation.
  - Edge cases: Verified correct grouping when workouts cross days and when datasets have missing or empty values.
- **Manual Inspection:**
  - Ran the CLI tool with sample data and checked output for correctness.

## How to Run
1. **Install Python 3.9+** (for `zoneinfo`)
2. **Run the CLI:**
   ```bash
   python src/main.py data/sleep.json data/workouts.json -o output.json
   ```
   - Use `-o <output file name>` to view merged output.
   - Use `-t <hours>` to set the sleep threshold for metrics.
3. **Run Tests:**
   ```bash
   python -m unittest discover -s tests -v
   ```

## Pipeline Recap
- Ingestion → Normalization → Alignment → Aggregation → Metric Calculation → Validation

---
