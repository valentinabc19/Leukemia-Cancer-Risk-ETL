"""Unit testing for the transformations of the leukemia dataset"""

import pandas as pd
import unittest
from unittest import mock
import os
import sys



ROOT_DIR = os.path.abspath(os.path.join(__file__, "../../../"))
if ROOT_DIR not in sys.path:
	sys.path.append(ROOT_DIR)
	
from airflow.functions.dimensional_model_transform import extract_medical_history, extract_region, extract_leukemia_facts, extract_patient_info, process_dimensions

class TestLeukemiaTransformations(unittest.TestCase):
	"""Test the transformations of th leukemia dataset."""

	def setUp(self):
		"""Set up a sample data archive for testing."""

		file_path = os.path.join(ROOT_DIR, "Leukemia-Cancer-Risk-ETL", "tests", "data", "test_leukemia_data.csv")
		self.df = pd.read_csv(file_path)