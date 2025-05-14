"""Unit testing for the loading of the API data and the leukemia data"""

import pandas as pd
import pytest
import os
import sys

from unittest import mock

ROOT_DIR = os.path.abspath(os.path.join(__file__, "../../../"))
if ROOT_DIR not in sys.path:
	sys.path.append(ROOT_DIR)
	
from airflow.functions.load_data import load_db_credentials, export_to_postgres

def test_load_db_credentials():
	return ""

def test_export_to_postgres():
	return ""