"""
CLI tool for merging health datasets with proper timezone normalization.

This tool handles the core workflow:
1. Load sleep data (UTC) and workout data (Local Time)
2. Normalize timestamps to a common standard (UTC)
3. Merge by UTC date, handling day boundary edge cases
4. Generate a consolidated report
"""

import json
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Any

from normalizer import TimestampNormalizer
from merger import DataMerger

class HealthDataAggregator:
    def __init__(self, sleep_file: str, workout_file: str, output_file: str = None):
        """
        Args:
            sleep_file: Path to sleep.json
            workout_file: Path to workouts.json
            output_file: Path to output file (optional)
        """
        self.sleep_file = Path(sleep_file)
        self.workout_file = Path(workout_file)
        self.output_file = Path(output_file) if output_file else None

    def load_json(self, file_path: Path) -> List[Dict[str, Any]]:
        """
        Load JSON data from file.

        Args:
            file_path: Path to JSON file

        Returns:
            Parsed JSON data
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        with open(file_path, "r") as f:
            return json.load(f)

    def normalize_data(
        self,
        sleep_records: List[Dict[str, Any]],
        workout_records: List[Dict[str, Any]],
    ) -> tuple:
        """
        Normalize both sleep and workout records.

        Args:
            sleep_records: Raw sleep data
            workout_records: Raw workout data

        Returns:
            Tuple of (normalized_sleep, normalized_workouts)
        """
        normalized_sleep = []
        normalized_workouts = []

        # Normalize sleep records
        for record in sleep_records:
            try:
                normalized = TimestampNormalizer.normalize_sleep_record(record)
                normalized_sleep.append(normalized)
            except ValueError as e:
                print(f"Warning: Skipping sleep record {record.get('id')}: {e}")

        # Normalize workout records
        for record in workout_records:
            try:
                normalized = TimestampNormalizer.normalize_workout_record(record)
                normalized_workouts.append(normalized)
            except ValueError as e:
                print(f"Warning: Skipping workout record {record.get('workout_id')}: {e}")

        return normalized_sleep, normalized_workouts

    def run(self) -> Dict[str, Any]:
        """
        Execute aggregation pipeline.

        Returns:
            Dictionary containing simple metadata and report of merged data
        """
        print("=" * 50)

        # 1. Load data
        print("\nLoading data...")
        try:
            sleep_records = self.load_json(self.sleep_file)
            workout_records = self.load_json(self.workout_file)
            print("Data loaded")
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error loading data: {e}")
            sys.exit(1)

        # 2. Normalize timestamps
        print("\nNormalizing timestamps to UTC...")
        try:
            normalized_sleep, normalized_workouts = self.normalize_data(
                sleep_records, workout_records
            )
            print("Timestamps normalized")
        except ValueError as e:
            print(f"Error normalizing data: {e}")
            sys.exit(1)

        # print("\nDEBUG sleep data:", normalized_sleep)
        # print("\nDEBUG workout data:", normalized_workouts)

        # 3. Merge by date
        print("\nMerging by date...")
        merged_data = DataMerger.merge_by_date(normalized_sleep, normalized_workouts)
        print("Dates merged")

        # 4. Generate report
        print("\nGenerating report...")
        report = DataMerger.generate_merged_report(merged_data)
        print(f"Report generated")

        # 5. Output results
        result = {
            "metadata": {
                "sleep_records_processed": len(normalized_sleep),
                "workout_records_processed": len(normalized_workouts),
                "dates_covered": len(merged_data),
                "date_range": {
                    "start": min(merged_data.keys()) if merged_data else None,
                    "end": max(merged_data.keys()) if merged_data else None,
                },
            },
            "detailed report": report,
        }

        if self.output_file:
            print(f"\nWriting result to {self.output_file}...")
            with open(self.output_file, "w") as f:
                json.dump(result, f, indent=2)
            print(f"Result written")

        return result

def main():
    """
    Main function
    """

    # 1. Parse input arguments
    parser = argparse.ArgumentParser(
        description="Merge health datasets with proper timezone normalization",
        epilog="""
Examples:
  %(prog)s data/sleep.json data/workouts.json
  %(prog)s data/sleep.json data/workouts.json -o output.json
        """,
    )

    parser.add_argument("sleep_file", help="Path to sleep data file (JSON, timestamps in UTC)")
    parser.add_argument("workout_file", help="Path to workout data file (JSON, timestamps in Local Time)")
    parser.add_argument("-o", "--output", help="Output file for merged data (optional)")

    args = parser.parse_args()

    # 2. Run aggregator
    aggregator = HealthDataAggregator(
        sleep_file=args.sleep_file,
        workout_file=args.workout_file,
        output_file=args.output,
    )

    result = aggregator.run()

    # 3. Calculate simple metric

    print("\nAggregation completed!\n")

if __name__ == "__main__":
    main()
