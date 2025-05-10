from api.extract_data import get_world_bank_data
import pandas as pd

def api_data_extraction() -> pd.DataFrame:
    """
    Function to extract data from an API endpoint.
    
    """
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

    data.to_csv("data/world_bank_data.csv", index=False)

    return pd.read_csv("data/world_bank_data.csv")
