import pandas as pd
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(__file__, "../../../"))
if ROOT_DIR not in sys.path:
	sys.path.append(ROOT_DIR)      

from api.extract_data import get_world_bank_data

def api_data_extraction() -> pd.DataFrame:
    """
    Function to extract data from an API endpoint or load it from a local file if it already exists.
    """
    # Ruta del archivo CSV
    file_path = os.path.join(ROOT_DIR, "data", "world_bank_data.csv")

    # Verificar si el archivo ya existe
    if os.path.exists(file_path):
        print(f"File {file_path} already exists. Loading data from the file.")
        return pd.read_csv(file_path)

    # Si el archivo no existe, realizar la extracción desde la API
    print(f"File {file_path} does not exist. Extracting data from the API.")
    indicators = {
        "EN.POP.SLUM.UR.ZS": "Population living in slums",
        "SI.POV.NAHC": "Poverty headcount ratio at national poverty lines",
        "NY.GDP.PCAP.CD": "GDP per capita",
        "SL.AGR.EMPL.ZS": "Employment in agriculture",
        "EN.ATM.PM25.MC.M3": "PM2.5 air pollution (mean annual exposure)",
        "EG.ELC.NUCL.ZS": "Electricity production from nuclear sources",
        "SN.ITK.DEFC.ZS": "Prevalence of undernourishment",
        "SN.ITK.MSFI.ZS": "Prevalence of moderate or severe food insecurity",
        "SH.ALC.PCAP.LI": "Total alcohol consumption per capita (liters of pure alcohol)",
        "EN.GHG.CO2.PC.CE.AR5": "Carbon dioxide (CO2) emissions per capita",
        "AG.CON.FERT.PT.ZS": "Fertilizer consumption"
    }

    data = get_world_bank_data(indicators)

    data.to_csv(file_path, index=False)
    print(f"Data extracted and saved to {file_path}.")

    return data