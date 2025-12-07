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

if __name__ == "__main__":
    unittest.main()
