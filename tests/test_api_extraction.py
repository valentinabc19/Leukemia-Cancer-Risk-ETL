"""Unit testing for the data extraction from the API"""

import pandas as pd
import unittest
from unittest import mock
import os
import sys


ROOT_DIR = os.path.abspath(os.path.join(__file__, "../../../"))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from airflow.functions.api_extraction import api_data_extraction

class TestApiDataExtraction(unittest.TestCase):
    """Test the API data extraction function."""

    @mock.patch("airflow.functions.api_extraction.pd.read_csv")
    @mock.patch("airflow.functions.api_extraction.os.path.exists")
    def test_load_existing_file(self, mock_exists, mock_read_csv):
        """Test when the CSV file already exists"""
        mock_exists.return_value = True
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        mock_read_csv.return_value = mock_df

        result = api_data_extraction()

        mock_exists.assert_called_once()
        mock_read_csv.assert_called_once()
        self.assertTrue(result.equals(mock_df))

    @mock.patch("airflow.functions.api_extraction.get_world_bank_data")
    @mock.patch("airflow.functions.api_extraction.pd.DataFrame.to_csv")
    @mock.patch("airflow.functions.api_extraction.os.path.exists")
    def test_extract_data_if_file_missing(self, mock_exists, mock_to_csv, mock_get_data):
        """Test when the CSV file does not exist"""
        mock_exists.return_value = False
        mock_df = pd.DataFrame({"indicator": ["X"], "value": [100]})
        mock_get_data.return_value = mock_df

        result = api_data_extraction()

        mock_exists.assert_called_once()
        mock_get_data.assert_called_once()
        mock_to_csv.assert_called_once_with(mock.ANY, index=False)
        self.assertTrue(result.equals(mock_df))

if __name__ == "__main__":
    unittest.main()
