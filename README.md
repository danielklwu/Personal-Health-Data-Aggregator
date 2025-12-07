# Personal-Health-Data-Aggregator
Health service that merges two datasets that don't perfectly align.

Goal:
Given sleep logs and workout logs in different time formats and time zones, build a system that:
- Normalizes timestamps
- Attributes each event to the correct user-local day
- Aggregates into daily summaries
- Computes a simple metric

Pipeline:
1. Ingestion: Load sleep.json and workouts.json
2. Normalization: Parse timestamps, normalize units
3. Align to matching timezone
4. Output & Validation