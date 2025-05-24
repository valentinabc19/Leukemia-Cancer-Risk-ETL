import requests
import pandas as pd

def get_country_codes():
    """
    Retrieve a list of valid country ISO codes from the World Bank API.

    The function sends a request to the World Bank's country endpoint and extracts
    all country codes (`id` field) for countries with a defined region (i.e., excluding
    aggregates or undefined regional entities such as "NA").

    Returns
    -------
    list of str
        A list of ISO 3166-1 alpha-3 country codes.
        If the request fails or the response is invalid, an empty list is returned.
    """

    url = "http://api.worldbank.org/v2/country?format=json&per_page=300"
    response = requests.get(url)
    if response.status_code == 200:
        json_data = response.json()
        if isinstance(json_data, list) and len(json_data) > 1:
            countries = [entry['id'] for entry in json_data[1] if entry['region']['id'] != 'NA']
            return countries
    return []

def get_world_bank_data(indicators_dict, start_year=2000, end_year=2025):
    """
    Fetch World Bank data for selected indicators and years.

    Parameters
    ----------
    indicators_dict : dict
        Dictionary with indicator codes as keys and readable names as values.
    start_year : int
        First year to fetch.
    end_year : int
        Last year to fetch.

    Returns
    -------
    pd.DataFrame
        Pivoted DataFrame with countries and years as rows and indicators as columns.
    """

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
