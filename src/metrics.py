"""
Metrics module for health data analysis.
"""
from typing import Dict, Any

class HealthMetrics:
    """Calculates health metrics from merged data."""

    @staticmethod
    def find_avg_calories_by_sleep(reports: Dict[str, Any], threshold: int) -> int:
        """
        Find average calories burned on days with sleep duration below threshold.

        Args:
            reports: Aggregated health report
            threshold: Sleep duration threshold in hours
        
        Returns:
            Average calories burned on low-sleep days
        """
        data = reports["detailed report"]
        total_calories, count = 0, 0
        for record in data:
            # Sleep tracked that day
            if record["sleep"]["count"]:
                sleep_hours = record["sleep"]["total_duration_hours"]
                if sleep_hours < threshold:
                    total_calories += record["workouts"]["total_calories_burned"]
                    count += 1

        return total_calories // count if count > 0 else 0