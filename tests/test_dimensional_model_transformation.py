"""Unit testing for the transformations of the leukemia dataset"""

import pandas as pd
import unittest
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(__file__, "../../../"))
if ROOT_DIR not in sys.path:
	sys.path.append(ROOT_DIR)
	
from airflow.functions.dimensional_model_transform import extract_medical_history, extract_region, extract_leukemia_facts, extract_patient_info, process_dimensions


class TestDimensionExtraction(unittest.TestCase):

    def setUp(self):
        """Set up the test case with a sample DataFrame."""
        self.df = pd.DataFrame({
            'id': [1],
            'age': [55],
            'gender': ['Male'],
            'country': ['Peru'],
            'wbc_count': [10000],
            'rbc_count': [4.5],
            'platelet_count': [250000],
            'hemoglobin_level': [13.5],
            'bone_marrow_blasts': [45],
            'bmi': [22.5],
            'leukemia_status': [1],
            'living_status': [1],
            'co2_emissions_per_capita': [1.5],
            'nuclear_energy_pct': [0.0],
            'agri_employment_pct': [30],
            'fertilizer_consumption': [110],
            'gdp_per_capita': [9500],
            'pm25_pollution': [18.2],
            'undernourishment_rate': [5.0],
            'alcohol_consumption_liters': [3.0],
            'genetic_mutation': [0],
            'family_history': [1],
            'smoking_status': [1],
            'alcohol_consumption': [0],
            'radiation_exposure': [1],
            'infection_history': [0],
            'chronic_illness': [1],
            'immune_disorders': [0],
            'socioeconomic_status': ['Low'],
            'urban_rural': ['Urban']
        })

    def test_extract_medical_history(self):
        """Test the transformation of the data to creste the medical history dimension."""
        
        result = extract_medical_history(self.df)
        self.assertEqual(len(result), 1)
        self.assertIn('medical_history_id', result.columns)
        self.assertEqual(result['medical_history_id'].iloc[0], 1)


    def test_extract_region(self):
        """Test the transformation of the data to create the region dimension."""

        result = extract_region(self.df)
        self.assertEqual(len(result), 1)
        self.assertIn('region_id', result.columns)
        self.assertEqual(result['country'].iloc[0], 'Peru')


    def test_extract_patient_info(self):
        """Test the transformation of the data to create the patient info dimension."""
        
        med_hist = extract_medical_history(self.df)
        medical_keys = self.df[[
            'genetic_mutation', 'family_history', 'smoking_status',
            'alcohol_consumption', 'radiation_exposure', 'infection_history',
            'chronic_illness', 'immune_disorders'
        ]].astype(str).apply(lambda x: '|'.join(x), axis=1)
        
        mapping = dict(zip(medical_keys, med_hist['medical_history_id']))
        
        result = extract_patient_info(self.df, mapping)
        
        self.assertIn('medical_history_id', result.columns)
        self.assertFalse(result['medical_history_id'].isna().any())
        self.assertEqual(result['medical_history_id'].iloc[0], 1)


    def test_extract_leukemia_facts(self):
        """Test the transformation of the data to create the leukemia facts table."""
        
        region_df = extract_region(self.df)
        region_ids = dict(zip(region_df['country'], region_df['region_id']))
        patient_ids = dict(zip(self.df['id'], self.df['id']))
        result = extract_leukemia_facts(self.df, patient_ids, region_ids)
        self.assertEqual(len(result), 1)
        self.assertIn('region_id', result.columns)
        self.assertEqual(result['patient_id'].iloc[0], 1)


    def test_process_dimensions(self):
        """Test the process_dimensions function to ensure it processes the DataFrame correctly."""
        
        result = process_dimensions(self.df)
        self.assertIsInstance(result, dict)
        self.assertIn('Fact_Leukemia', result)
        self.assertIn('Dim_Region', result)
        self.assertIn('Dim_PatientInfo', result)
        self.assertIn('Dim_MedicalHistory', result)
        self.assertEqual(len(result['Fact_Leukemia']), 1)
        self.assertEqual(len(result['Dim_Region']), 1)

if __name__ == '__main__':
    unittest.main()