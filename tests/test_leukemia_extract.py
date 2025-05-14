"""Unit testing for the data extraction from the leukemia dataset"""

import pandas as pd
import pytest
import os
import sys

from unittest import mock

ROOT_DIR = os.path.abspath(os.path.join(__file__, "../../../"))
if ROOT_DIR not in sys.path:
	sys.path.append(ROOT_DIR)
	
from airflow.functions.leukemia_extract import extract_data

def test_extract_data():
	return ""