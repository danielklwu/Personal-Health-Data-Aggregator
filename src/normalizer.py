"""
Normalization module for converting timestamps to a standard format (UTC).

This module handles timezone conversions and timestamp standardization,
with special attention to day boundary edge cases.
"""

from datetime import datetime, timezone
from typing import Dict, Any, Tuple
from zoneinfo import ZoneInfo


class TimestampNormalizer:
    """Handles timestamp normalization and timezone conversions."""

    # Timezone mappings for common timezones
    TIMEZONE_MAP = {
        "UTC": ZoneInfo("UTC"),
        "PST": ZoneInfo("America/Los_Angeles"),
        "EST": ZoneInfo("America/New_York"),
        "CST": ZoneInfo("America/Chicago"),
        "MST": ZoneInfo("America/Denver"),
        "GMT": ZoneInfo("UTC"),
    }

    @staticmethod
    def parse_utc_timestamp(timestamp_str: str) -> datetime:
        """
        Parse an ISO UTC timestamp.

        Args:
            timestamp_str: ISO format timestamp (e.g. '2023-10-01T08:00:00Z')

        Returns:
            datetime object in UTC
        """
        try:
            return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        except ValueError as e:
            raise ValueError(f"Invalid UTC timestamp format: {timestamp_str}") from e

    @staticmethod
    def parse_local_timestamp(
        timestamp_str: str, tz_name: str
    ) -> Tuple[datetime, str]:
        """
        Parse a local timestamp string and convert to UTC.

        Args:
            timestamp_str: Timestamp string (e.g. '2023-10-01 15:30:00')
            tz_name: Timezone name (e.g. 'PST')

        Returns:
            Tuple of (datetime in UTC, original date in local time)
        """
        try:
            # Parse timestamp
            local_dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

            # Get local timezone
            if tz_name not in TimestampNormalizer.TIMEZONE_MAP:
                raise ValueError(f"Unknown timezone: {tz_name}")

            tz = TimestampNormalizer.TIMEZONE_MAP[tz_name]

            # Add timezone to local datetime and convert to UTC
            local_aware = local_dt.replace(tzinfo=tz)
            utc_dt = local_aware.astimezone(timezone.utc)

            # Store the original local date (YYYY-MM-DD)
            local_date = local_dt.strftime("%Y-%m-%d")

            return utc_dt, local_date
        except ValueError as e:
            raise ValueError(
                f"Invalid local timestamp format or timezone: {timestamp_str}, {tz_name}"
            ) from e

    @staticmethod
    def get_utc_date(dt: datetime) -> str:
        """
        Extract the date (YYYY-MM-DD) from a UTC datetime.

        Args:
            dt: datetime object in UTC

        Returns:
            Date string in YYYY-MM-DD format
        """
        return dt.strftime("%Y-%m-%d")

    @staticmethod
    def normalize_sleep_record(record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize a sleep record to UTC timestamps.

        Args:
            record: Sleep record with UTC timestamps

        Returns:
            Normalized record with additional metadata
        """
        try:
            start_utc = TimestampNormalizer.parse_utc_timestamp(record["start_time"])
            end_utc = TimestampNormalizer.parse_utc_timestamp(record["end_time"])

            return {
                **record,
                "start_time_utc": start_utc.isoformat(),
                "end_time_utc": end_utc.isoformat(),
                "date": TimestampNormalizer.get_utc_date(start_utc),
                "source": "sleep",
            }
        except ValueError as e:
            raise ValueError(f"Failed to normalize sleep record: {e}") from e

    @staticmethod
    def normalize_workout_record(record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize a workout record, converting local time to UTC.

        Args:
            record: Workout record with local timestamps

        Returns:
            Normalized record with UTC timestamp and metadata
        """
        try:
            utc_dt, local_date = TimestampNormalizer.parse_local_timestamp(record["timestamp"], record["timezone"])

            return {
                **record,
                "timestamp_utc": utc_dt.isoformat(),
                "timestamp_local": record["timestamp"],
                "local_date": local_date,
                "date": TimestampNormalizer.get_utc_date(utc_dt),
                "source": "workout",
            }
        except ValueError as e:
            raise ValueError(f"Failed to normalize workout record: {e}") from e
