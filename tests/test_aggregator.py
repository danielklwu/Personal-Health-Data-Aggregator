"""
Unit tests for the health data aggregator.
- Timestamp normalization and parsing
- Timezone conversions
- Day boundary edge cases
"""

import unittest
from datetime import datetime

from src.normalizer import TimestampNormalizer
from src.merger import DataMerger
from src.metrics import HealthMetrics


class TestTimestampNormalizer(unittest.TestCase):
    """Test timestamp normalization functions."""

    def test_parse_utc_timestamp_valid(self):
        """Test parsing valid UTC timestamps."""
        result = TimestampNormalizer.parse_utc_timestamp("2023-10-01T08:00:00Z")
        self.assertEqual(result.year, 2023)
        self.assertEqual(result.month, 10)
        self.assertEqual(result.day, 1)
        self.assertEqual(result.hour, 8)
        self.assertEqual(result.minute, 0)

    def test_parse_utc_timestamp_with_offset(self):
        """Test parsing UTC timestamps with +00:00 format."""
        result = TimestampNormalizer.parse_utc_timestamp("2023-10-01T08:00:00+00:00")
        self.assertEqual(result.year, 2023)
        self.assertEqual(result.month, 10)

    def test_parse_utc_timestamp_invalid(self):
        """Test that invalid UTC timestamps raise ValueError."""
        with self.assertRaises(ValueError):
            TimestampNormalizer.parse_utc_timestamp("INVALID")

    def test_parse_local_timestamp_pst(self):
        """Test parsing local timestamps in PST."""
        # Use January date when PST (UTC-8) is in effect
        utc_dt, local_date = TimestampNormalizer.parse_local_timestamp(
            "2023-01-01 15:30:00", "PST"
        )
        # 15:30 PST = 23:30 UTC
        self.assertEqual(utc_dt.hour, 23)
        self.assertEqual(utc_dt.minute, 30)
        self.assertEqual(local_date, "2023-01-01")

    def test_parse_local_timestamp_day_boundary(self):
        """Test day boundary crossing: 11 PM PST -> Next day UTC."""
        utc_dt, local_date = TimestampNormalizer.parse_local_timestamp(
            "2023-10-01 23:45:00", "PST"
        )
        # 2023-10-01 23:45:00 PST = 2023-10-02 06:45:00 UTC
        self.assertEqual(utc_dt.day, 2) 
        self.assertEqual(utc_dt.month, 10) # Oct 2
        self.assertEqual(local_date, "2023-10-01")  # Oct 1

    def test_parse_local_timestamp_invalid_timezone(self):
        """Test that invalid timezone raises ValueError."""
        with self.assertRaises(ValueError):
            TimestampNormalizer.parse_local_timestamp("2023-10-01 15:30:00", "INVALID")

    def test_get_utc_date(self):
        """Test extracting date from datetime."""
        dt = datetime.fromisoformat("2023-10-01T08:00:00+00:00")
        result = TimestampNormalizer.get_utc_date(dt)
        self.assertEqual(result, "2023-10-01")

    def test_normalize_sleep_record(self):
        """Test normalizing a sleep record."""
        record = {
            "id": "sleep_001",
            "device": "Sleep Tracker",
            "start_time": "2023-10-01T08:00:00Z",
            "end_time": "2023-10-01T16:00:00Z",
            "duration_hours": 8,
            "quality_score": 85,
        }
        result = TimestampNormalizer.normalize_sleep_record(record)
        self.assertEqual(result["date"], "2023-10-01")
        self.assertEqual(result["source"], "sleep")
        self.assertIn("start_time_utc", result)
        self.assertIn("end_time_utc", result)

    def test_normalize_workout_record(self):
        """Test normalizing a workout record with timezone conversion."""
        record = {
            "workout_id": "w001",
            "app": "Fitness Tracker",
            "timestamp": "2023-10-01 15:30:00",
            "timezone": "PST",
            "exercise_type": "Running",
            "duration_minutes": 45,
        }
        # 15:30 PST = 23:30 UTC (same day)
        result = TimestampNormalizer.normalize_workout_record(record)
        self.assertEqual(result["local_date"], "2023-10-01")
        self.assertEqual(result["source"], "workout")
        self.assertEqual(result["date"], "2023-10-01")

    def test_normalize_workout_record_day_boundary(self):
        """Test workout timestamp crossing day boundary during normalization."""
        record = {
            "workout_id": "w002",
            "app": "Fitness Tracker",
            "timestamp": "2023-10-01 23:45:00",
            "timezone": "PST",
            "exercise_type": "Yoga",
            "duration_minutes": 60,
        }
        # Oct 1 23:45 PST = Oct 2 7:45 UTC
        result = TimestampNormalizer.normalize_workout_record(record)
        self.assertEqual(result["local_date"], "2023-10-01")
        self.assertEqual(result["date"], "2023-10-02")


class TestDataMerger(unittest.TestCase):
    """Test data merging functions."""

    def setUp(self):
        """Set up test data."""
        self.sleep_records = [
            {
                "id": "sleep_001",
                "date": "2023-10-01",
                "duration_hours": 8,
                "quality_score": 85,
                "source": "sleep",
            },
            {
                "id": "sleep_002",
                "date": "2023-10-02",
                "duration_hours": 8,
                "quality_score": 78,
                "source": "sleep",
            },
        ]

        self.workout_records = [
            {
                "workout_id": "w001",
                "date": "2023-10-01",
                "duration_minutes": 45,
                "calories_burned": 520,
                "source": "workout",
            },
            {
                "workout_id": "w002",
                "date": "2023-10-02",
                "duration_minutes": 60,
                "calories_burned": 600,
                "source": "workout",
            },
        ]

    def test_merge_by_date_basic(self):
        """Test basic merging by date."""
        result = DataMerger.merge_by_date(self.sleep_records, self.workout_records)
        self.assertEqual(len(result), 2)
        self.assertIn("2023-10-01", result)
        self.assertIn("2023-10-02", result)

    def test_merge_by_date_grouping(self):
        """Test that records are correctly grouped by date."""
        result = DataMerger.merge_by_date(self.sleep_records, self.workout_records)
        self.assertEqual(len(result["2023-10-01"]["sleep"]), 1)
        self.assertEqual(len(result["2023-10-01"]["workouts"]), 1)
        self.assertEqual(len(result["2023-10-02"]["sleep"]), 1)
        self.assertEqual(len(result["2023-10-02"]["workouts"]), 1)

    def test_merge_by_date_missing_data(self):
        """Test merging when some dates have only one data type."""
        # No workouts on Oct 3rd
        sleep_records = self.sleep_records + [
            {
                "id": "sleep_003",
                "date": "2023-10-03",
                "duration_hours": 8,
                "quality_score": 92,
                "source": "sleep",
            }
        ]
        result = DataMerger.merge_by_date(sleep_records, self.workout_records)
        self.assertEqual(len(result["2023-10-03"]["sleep"]), 1)
        self.assertEqual(len(result["2023-10-03"]["workouts"]), 0)

    def test_create_daily_summary(self):
        """Test daily summary creation."""
        summary = DataMerger.create_daily_summary(
            "2023-10-01",
            [self.sleep_records[0]],
            [self.workout_records[0]],
        )

        self.assertEqual(summary["date"], "2023-10-01")
        self.assertEqual(summary["sleep"]["count"], 1)
        self.assertEqual(summary["sleep"]["total_duration_hours"], 8)
        self.assertEqual(summary["workouts"]["count"], 1)
        self.assertEqual(summary["workouts"]["total_duration_minutes"], 45)
        self.assertEqual(summary["workouts"]["total_calories_burned"], 520)

    def test_create_daily_summary_empty_data(self):
        """Test daily summary with empty records."""
        summary = DataMerger.create_daily_summary("2023-10-01", [], [])
        self.assertEqual(summary["sleep"]["count"], 0)
        self.assertEqual(summary["workouts"]["count"], 0)

    def test_create_daily_summary_quality_score(self):
        """Test average quality score calculation."""
        sleep_recs = [
            {"duration_hours": 8, "quality_score": 80, "source": "sleep"},
            {"duration_hours": 8, "quality_score": 90, "source": "sleep"},
        ]
        summary = DataMerger.create_daily_summary("2023-10-01", sleep_recs, [])
        self.assertEqual(summary["sleep"]["average_quality_score"], 85.0)

    def test_generate_merged_report_sorting(self):
        """Test that report is sorted by date."""
        merged = DataMerger.merge_by_date(self.sleep_records, self.workout_records)
        report = DataMerger.generate_merged_report(merged)

        self.assertEqual(report[0]["date"], "2023-10-01")
        self.assertEqual(report[1]["date"], "2023-10-02")


class TestHealthMetrics(unittest.TestCase):
    """Test health metrics calculations."""

    def setUp(self):
        """Set up test data."""
        # Create a sample report structure
        self.sample_report = {
            "metadata": {
                "sleep_records_processed": 4,
                "workout_records_processed": 4,
                "dates_covered": 4,
            },
            "detailed report": [
                {
                    "date": "2023-10-01",
                    "sleep": {
                        "count": 1,
                        "total_duration_hours": 8,
                        "average_quality_score": 85.0,
                    },
                    "workouts": {
                        "count": 1,
                        "total_duration_minutes": 45,
                        "total_calories_burned": 500,
                    },
                },
                {
                    "date": "2023-10-02",
                    "sleep": {
                        "count": 1,
                        "total_duration_hours": 6,  
                        "average_quality_score": 70.0,
                    },
                    "workouts": {
                        "count": 2,
                        "total_duration_minutes": 90,
                        "total_calories_burned": 800,
                    },
                },
                {
                    "date": "2023-10-03",
                    "sleep": {
                        "count": 1,
                        "total_duration_hours": 5.5,  
                        "average_quality_score": 65.0,
                    },
                    "workouts": {
                        "count": 1,
                        "total_duration_minutes": 60,
                        "total_calories_burned": 600,
                    },
                },
                {
                    "date": "2023-10-04",
                    "sleep": {
                        "count": 0,  # No sleep data
                        "total_duration_hours": 0,
                        "average_quality_score": 0,
                    },
                    "workouts": {
                        "count": 1,
                        "total_duration_minutes": 30,
                        "total_calories_burned": 300,
                    },
                },
            ],
        }

    def test_find_avg_calories_by_sleep_below_threshold(self):
        """Test average calories calculation for days below sleep threshold."""
        # Default threshold of 7 hours should catch 10/2 (6h) and 10/3 (5.5h)
        result = HealthMetrics.find_avg_calories_by_sleep(
            self.sample_report, threshold=7
        )
        # Average calories = (800 + 600) / 2 = 700
        self.assertEqual(result, 700)

    def test_find_avg_calories_by_sleep_with_higher_threshold(self):
        """Test with higher threshold to include more days."""
        # Threshold of 9 hours should catch all days with sleep data
        result = HealthMetrics.find_avg_calories_by_sleep(
            self.sample_report, threshold=9
        )
        # Average calories = (500 + 800 + 600) / 3 = 633
        self.assertEqual(result, 633)

    def test_find_avg_calories_by_sleep_no_days_below_threshold(self):
        """Test when no days are below threshold."""
        # Threshold of 5 hours - no days below this
        result = HealthMetrics.find_avg_calories_by_sleep(
            self.sample_report, threshold=5
        )
        self.assertEqual(result, 0)

    def test_find_avg_calories_by_sleep_with_zero_sleep_days(self):
        """Test that days with zero sleep (no data) are ignored."""
        # Days with no sleep data should not be included
        result = HealthMetrics.find_avg_calories_by_sleep(
            self.sample_report, threshold=7
        )
        # 10/4 with 0 hours should not be counted
        self.assertEqual(result, 700)

    def test_find_avg_calories_by_sleep_empty_report(self):
        """Test with empty report."""
        empty_report = {
            "metadata": {"sleep_records_processed": 0},
            "detailed report": [],
        }
        result = HealthMetrics.find_avg_calories_by_sleep(empty_report, threshold=7)
        self.assertEqual(result, 0)

    def test_find_avg_calories_by_sleep_edge_case_exact_threshold(self):
        """Test behavior when sleep equals threshold exactly."""
        report = {
            "detailed report": [
                {
                    "date": "2023-10-01",
                    "sleep": {
                        "count": 1,
                        "total_duration_hours": 7,  # Exactly at threshold
                    },
                    "workouts": {"total_calories_burned": 500},
                }
            ]
        }
        # At threshold (7 == 7) should NOT be included (only < threshold)
        result = HealthMetrics.find_avg_calories_by_sleep(report, threshold=7)
        self.assertEqual(result, 0)

if __name__ == "__main__":
    unittest.main()
