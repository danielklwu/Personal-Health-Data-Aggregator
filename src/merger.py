"""
Merger module for combining normalized health datasets.
"""

from typing import Dict, List, Any
from collections import defaultdict

class DataMerger:
    """Handles merging of normalized health datasets by date."""

    @staticmethod
    def merge_by_date(normalized_sleep: List[Dict[str, Any]], normalized_workouts: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Merge sleep and workout data by UTC date.

        Args:
            normalized_sleep: List of normalized sleep records
            normalized_workouts: List of normalized workout records

        Returns:
            Dictionary keyed by date (YYYY-MM-DD) with merged data
        """

        merged = defaultdict(lambda: {"sleep": [], "workouts": []})

        # Add sleep and workout records to UTC date
        for record in normalized_sleep:
            date = record["date"]
            merged[date]["sleep"].append(record)
        for record in normalized_workouts:
            date = record["date"]
            merged[date]["workouts"].append(record)

        # Convert to regular dict
        return dict(merged)

    @staticmethod
    def create_daily_summary(date: str, sleep_records: List[Dict[str, Any]], workout_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create a summary for a single day.

        Args:
            date: Date string (YYYY-MM-DD)
            sleep_records: List of sleep records for this date
            workout_records: List of workout records for this date

        Returns:
            Summary dictionary with aggregated data
        """

        # summary of sleep for that date with relevant metrics
        sleep_summary = {
            "count": len(sleep_records),
            "total_duration_hours": sum(r.get("duration_hours", 0) for r in sleep_records),
            "average_quality_score": (
                sum(r.get("quality_score", 0) for r in sleep_records) / len(sleep_records)
                if sleep_records
                else 0
            ),
            "records": sleep_records,
        }

        # summary of workout for that date with relevant metrics
        workout_summary = {
            "count": len(workout_records),
            "total_duration_minutes": sum(
                r.get("duration_minutes", 0) for r in workout_records
            ),
            "total_calories_burned": sum(
                r.get("calories_burned", 0) for r in workout_records
            ),
            "records": workout_records,
        }

        return {
            "date": date,
            "sleep": sleep_summary,
            "workouts": workout_summary,
        }

    @staticmethod
    def generate_merged_report(merged_data: Dict[str, Dict[str, List[Dict[str, Any]]]]) -> List[Dict[str, Any]]:
        """
        Generate a detailed report from merged data.

        Args:
            merged_data: Output from merge_by_date()

        Returns:
            List of daily summary dictionaries, sorted by date
        """

        report = []

        for date in sorted(merged_data.keys()):
            summary = DataMerger.create_daily_summary(
                date,
                merged_data[date]["sleep"],
                merged_data[date]["workouts"],
            )
            report.append(summary)

        return report
