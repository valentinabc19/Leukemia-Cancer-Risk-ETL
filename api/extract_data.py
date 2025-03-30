import requests
import pandas as pd

def get_country_codes():
    url = "http://api.worldbank.org/v2/country?format=json&per_page=300"
    response = requests.get(url)
    if response.status_code == 200:
        json_data = response.json()
        if isinstance(json_data, list) and len(json_data) > 1:
            countries = [entry['id'] for entry in json_data[1] if entry['region']['id'] != 'NA']
            return countries
    return []

def get_world_bank_data(indicators_dict, start_year=2000, end_year=2025):
    country_codes = get_country_codes()
    countries = ';'.join(country_codes)
    base_url = "http://api.worldbank.org/v2/country/{}/indicator/{}?format=json&date={}:{}&per_page=1000&page={}"
    data = []
    
    for indicator, name in indicators_dict.items():
        page = 1
        while True:
            url = base_url.format(countries, indicator, start_year, end_year, page)
            response = requests.get(url)
            if response.status_code != 200:
                break
            json_data = response.json()
            if not isinstance(json_data, list) or len(json_data) <= 1 or not json_data[1]:
                break
            
            for entry in json_data[1]:
                if entry and 'value' in entry and entry['value'] is not None:
                    data.append({
                        'Country': entry['country']['value'],
                        'Year': entry['date'],
                        'Indicator': name,
                        'Value': entry['value']
                    })
            
            page += 1
    
    df = pd.DataFrame(data)
    df_pivot = df.pivot_table(index=['Country', 'Year'], columns='Indicator', values='Value').reset_index()
    return df_pivot

# Indicadores requeridos
indicators = {
    "EN.POP.SLUM.UR.ZS": "Population living in slums",
    "SI.POV.NAHC": "Poverty headcount ratio at national poverty lines",
    "NY.GDP.PCAP.CD": "GDP per capita (PIB)",
    "SL.AGR.EMPL.ZS": "Employment in agriculture (% of total employment)",
    "EN.ATM.PM25.MC.M3": "PM2.5 air pollution (mean annual exposure)",
    "EG.ELC.NUCL.ZS": "Electricity production from nuclear sources",
    "SN.ITK.DEFC.ZS": "Prevalence of undernourishment",
    "FI.FSI.FOOD.ZS": "Prevalence of moderate or severe food insecurity",
    "SH.ALC.PCAP.LI": "Total alcohol consumption per capita (liters of pure alcohol)",
    "EN.ATM.CO2E.PC": "Carbon dioxide (CO2) emissions per capita",
    "AG.LND.IRIG.AG.ZS": "Agricultural irrigated land",
    "AG.CON.FERT.PT.ZS": "Fertilizer consumption"
}

# Obtener datos
df = get_world_bank_data(indicators)

# Guardar en CSV
df.to_csv("world_bank_data.csv", index=False)
print("Datos guardados en world_bank_data.csv")