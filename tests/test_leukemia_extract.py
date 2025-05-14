"""Unit testing for the data extraction from the leukemia dataset"""

import pandas as pd
import unittest
from unittest import mock
import os
import sys
from io import StringIO

ROOT_DIR = os.path.abspath(os.path.join(__file__, "../../../"))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from airflow.functions.leukemia_extract import extract_data

class TestLeukemiaDataExtraction(unittest.TestCase):

    @mock.patch('airflow.functions.leukemia_extract.pd.read_sql')
    @mock.patch('airflow.functions.leukemia_extract.create_engine')
    @mock.patch('airflow.functions.leukemia_extract.open', create=True)
    def test_extract_data(self, mock_open, mock_create_engine, mock_read_sql):
        mock_open.return_value.__enter__.return_value.read.return_value = """
        {
            "db_host": "test_host",
            "db_name": "test_db",
            "db_user": "test_user",
            "db_password": "test_pass"
        }
        """
        expected_df = pd.DataFrame({
            'patient_id': [1, 2],
            'age': [30, 40],
        })
        mock_read_sql.return_value = expected_df

        result =  extract_data()

        pd.testing.assert_frame_equal(result, expected_df)
        mock_create_engine.assert_called_once_with(
            "postgresql://test_user:test_pass@test_host:5432/test_db"
        )
        mock_read_sql.assert_called_once()



        


		
		