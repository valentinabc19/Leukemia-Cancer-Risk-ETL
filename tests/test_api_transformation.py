"""Unit testing for the data transformation from the API"""

import pandas as pd
import unittest
from unittest import mock
import os
import sys


ROOT_DIR = os.path.abspath(os.path.join(__file__, "../../../"))
if ROOT_DIR not in sys.path:
	sys.path.append(ROOT_DIR)
	
from airflow.functions.api_transformations import standardize_country_names, filter_countries, rename_columns, handle_missing_values, drop_high_null_columns, impute_nuclear_energy, remove_outliers, latest_year_data, process_world_bank_data

class TestApiTranformations(unittest.TestCase):
	"""Test the API data transformation functions."""

	def setUp(self):
		"""Set up a sample data archive for testing."""

		file_path = os.path.join(ROOT_DIR, "Leukemia-Cancer-Risk-ETL", "tests", "data", "test_api_data.csv")
		self.df = pd.read_csv(file_path)

	def test_standardize_country_names(self):
		"""Test the standardization of country names."""

		result = standardize_country_names(self.df.copy())
		expected = ['usa', 'turkey', 'germany', 'france']
		self.assertListEqual(list(result['Country']), expected)
	
	def test_filter_countries(self):
		"""Test the filtering of countries."""

		df = standardize_country_names(self.df.copy())
		result = filter_countries(df)
		expected_countries = ['usa', 'turkey', 'germany', 'france']
		self.assertTrue(set(result['Country']).issubset(expected_countries))
	
	def test_rename_columns(self):
		"""Test the renaming of the columns."""

		df = standardize_country_names(self.df.copy())
		df = filter_countries(df)
		result = rename_columns(df)
		self.assertIn('co2_emissions_per_capita', result.columns)
		self.assertIn('fertilizer_consumption', result.columns)

	def test_handle_missing_values(self):
		"""Test the handling of missing values."""

		df = standardize_country_names(self.df.copy())
		df = rename_columns(df)
		df['Country'] = df['country']
		result = handle_missing_values(df)
		self.assertFalse(result['alcohol_consumption_liters'].isna().any())

	def test_drop_high_null_columns(self):
		"""Test the dropping of columns with high nulls values."""

		df = rename_columns(self.df.copy())
		result = drop_high_null_columns(df)
		self.assertNotIn('slum_population_pct', result.columns)
		self.assertNotIn('food_insecurity_rate', result.columns)
	
	def test_impute_nuclear_energy(self):
		"""Test the imputation of nuclear energy values."""

		df = standardize_country_names(self.df.copy())
		df = rename_columns(df)
		result = impute_nuclear_energy(df)
		self.assertFalse(result['nuclear_energy_pct'].isna().any())
		
	def test_remove_outliers(self):
		"""Test the removal of outliers."""

		df = standardize_country_names(self.df.copy())
		df = rename_columns(df)
		df['Country'] = df['country']
		df = df[df['country'] == 'france']
		result = remove_outliers(df)
		self.assertLessEqual(result['fertilizer_consumption'].max(), 600)

	def test_latest_year_data(self):
		"""Test the filtering of the latest year data."""

		df = rename_columns(self.df.copy())
		df['country'] = ['usa', 'usa', 'france', 'france']
		df['year'] = [2021, 2022, 2021, 2022]
		result = latest_year_data(df)
		self.assertEqual(len(result), 2)
		self.assertTrue(result['country'].tolist(), ['usa', 'france'])
		self.assertNotIn('year', result.columns)

	def test_process_world_bank_data(self):
		"""Test the processing of the world bank data."""

		result = process_world_bank_data(self.df.copy())

		columns_expected_clean = [
			'nuclear_energy_pct',
			'alcohol_consumption_liters',
			'fertilizer_consumption',
			'undernourishment_rate',
			'pm25_pollution'
		]
		for col in columns_expected_clean:
			self.assertFalse(result[col].isna().any(), f"Column {col} still contains missing values after processing.")

		self.assertFalse(result['fertilizer_consumption'].isna().any())
		self.assertIn('co2_emissions_per_capita', result.columns)
		self.assertNotIn('year', result.columns)
		self.assertFalse(result.isna().any().any())
		
if __name__ == '__main__':
	unittest.main()