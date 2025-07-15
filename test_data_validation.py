import unittest
import pandas as pd
import os
import streamlit as st

CLEAN_DATA_DIR = "data/cleaned"
REQUIRED_COLUMNS = ["timestamp", "street_name", "direction", "bike_count"]

class TestCleanedData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        files = sorted([f for f in os.listdir(CLEAN_DATA_DIR) if f.endswith(".csv")])
        if not files:
            raise FileNotFoundError("No cleaned data files found.")
        latest_file = os.path.join(CLEAN_DATA_DIR, files[-1])
        cls.df = pd.read_csv(latest_file, parse_dates=["timestamp"])
        st.info(f"Testing file: {latest_file}")

    def test_required_columns_exist(self):
        for col in REQUIRED_COLUMNS:
            self.assertIn(col, self.df.columns, f"Missing required column: {col}")

    def test_no_nulls_in_critical_columns(self):
        for col in REQUIRED_COLUMNS:
            null_count = self.df[col].isnull().sum()
            self.assertEqual(null_count, 0, f"Null values found in {col}: {null_count}")

    def test_bike_count_positive(self):
        invalid = (self.df["bike_count"] < 0).sum()
        self.assertEqual(invalid, 0, f"Found negative bike counts: {invalid}")

    def test_minimum_record_count(self):
        self.assertGreater(len(self.df), 100, "Insufficient records in dataset")

if __name__ == "__main__":
    unittest.main()
