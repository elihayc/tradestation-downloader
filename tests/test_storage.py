"""Tests for storage module."""

import pandas as pd

from tradestation.models import StorageFormat
from tradestation.storage import (
    SingleFileStorage,
    DailyPartitionedStorage,
    MonthlyPartitionedStorage,
    create_storage,
    detect_storage_format,
)


def create_sample_df(dates):
    """Create a sample OHLCV DataFrame."""
    return pd.DataFrame({
        "datetime": pd.to_datetime(dates),
        "open": [100.0] * len(dates),
        "high": [101.0] * len(dates),
        "low": [99.0] * len(dates),
        "close": [100.5] * len(dates),
        "volume": [1000] * len(dates),
    })


class TestSingleFileStorage:
    """Tests for SingleFileStorage."""

    def test_save_and_load(self, temp_data_dir):
        storage = SingleFileStorage(temp_data_dir)
        df = create_sample_df(["2024-01-01 09:30", "2024-01-01 09:31"])

        storage.save("ES", df)
        loaded = storage.load("ES")

        assert loaded is not None
        assert len(loaded) == 2
        # datetime is the index (datetime_index=True by default)
        assert list(loaded.columns) == ["open", "high", "low", "close", "volume"]
        assert loaded.index.name == "datetime"

    def test_load_nonexistent(self, temp_data_dir):
        storage = SingleFileStorage(temp_data_dir)
        assert storage.load("NONEXISTENT") is None

    def test_list_symbols(self, temp_data_dir):
        storage = SingleFileStorage(temp_data_dir)
        df = create_sample_df(["2024-01-01 09:30"])

        storage.save("ES", df)
        storage.save("NQ", df)

        symbols = storage.list_symbols()
        assert set(symbols) == {"ES", "NQ"}

    def test_get_file_size(self, temp_data_dir):
        storage = SingleFileStorage(temp_data_dir)
        df = create_sample_df(["2024-01-01 09:30"])

        storage.save("ES", df)
        size = storage.get_file_size("ES")

        assert size > 0


class TestDailyPartitionedStorage:
    """Tests for DailyPartitionedStorage."""

    def test_save_and_load(self, temp_data_dir):
        storage = DailyPartitionedStorage(temp_data_dir)
        df = create_sample_df([
            "2024-01-01 09:30",
            "2024-01-01 09:31",
            "2024-01-02 09:30",
        ])

        storage.save("ES", df)
        loaded = storage.load("ES")

        assert loaded is not None
        assert len(loaded) == 3

    def test_creates_partitions(self, temp_data_dir):
        storage = DailyPartitionedStorage(temp_data_dir)
        df = create_sample_df([
            "2024-01-15 09:30",
            "2024-01-16 09:30",
        ])

        storage.save("ES", df)

        # Check partition directories exist (datetime_index=True adds _index_1 suffix)
        assert (temp_data_dir / "ES_index_1" / "year=2024" / "month=01" / "day=15").exists()
        assert (temp_data_dir / "ES_index_1" / "year=2024" / "month=01" / "day=16").exists()


class TestMonthlyPartitionedStorage:
    """Tests for MonthlyPartitionedStorage."""

    def test_save_and_load(self, temp_data_dir):
        storage = MonthlyPartitionedStorage(temp_data_dir)
        df = create_sample_df([
            "2024-01-15 09:30",
            "2024-02-15 09:30",
        ])

        storage.save("ES", df)
        loaded = storage.load("ES")

        assert loaded is not None
        assert len(loaded) == 2

    def test_creates_partitions(self, temp_data_dir):
        storage = MonthlyPartitionedStorage(temp_data_dir)
        df = create_sample_df([
            "2024-01-15 09:30",
            "2024-02-15 09:30",
        ])

        storage.save("ES", df)

        # Check partition directories exist (datetime_index=True adds _index_1 suffix)
        assert (temp_data_dir / "ES_index_1" / "year_month=2024-01").exists()
        assert (temp_data_dir / "ES_index_1" / "year_month=2024-02").exists()


class TestCreateStorage:
    """Tests for create_storage factory function."""

    def test_creates_single(self, temp_data_dir):
        storage = create_storage(StorageFormat.SINGLE, temp_data_dir)
        assert isinstance(storage, SingleFileStorage)

    def test_creates_daily(self, temp_data_dir):
        storage = create_storage(StorageFormat.DAILY, temp_data_dir)
        assert isinstance(storage, DailyPartitionedStorage)

    def test_creates_monthly(self, temp_data_dir):
        storage = create_storage(StorageFormat.MONTHLY, temp_data_dir)
        assert isinstance(storage, MonthlyPartitionedStorage)


class TestDetectStorageFormat:
    """Tests for detect_storage_format function."""

    def test_detects_single(self, temp_data_dir):
        storage = SingleFileStorage(temp_data_dir)
        df = create_sample_df(["2024-01-01 09:30"])
        storage.save("ES", df)

        detected = detect_storage_format(temp_data_dir)
        assert detected == StorageFormat.SINGLE

    def test_detects_daily(self, temp_data_dir):
        storage = DailyPartitionedStorage(temp_data_dir)
        df = create_sample_df(["2024-01-01 09:30"])
        storage.save("ES", df)

        detected = detect_storage_format(temp_data_dir)
        assert detected == StorageFormat.DAILY

    def test_detects_monthly(self, temp_data_dir):
        storage = MonthlyPartitionedStorage(temp_data_dir)
        df = create_sample_df(["2024-01-01 09:30"])
        storage.save("ES", df)

        detected = detect_storage_format(temp_data_dir)
        assert detected == StorageFormat.MONTHLY

    def test_empty_directory(self, temp_data_dir):
        detected = detect_storage_format(temp_data_dir)
        assert detected == StorageFormat.SINGLE
