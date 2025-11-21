import pandas as pd

eu_27 = ["Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czech Republic", "Denmark",
         "Estonia", "Finland", "France", "Germany", "Greece", "Hungary", "Ireland", "Italy", "Latvia",
         "Lithuania", "Luxembourg", "Malta", "Netherlands", "Poland", "Portugal", "Romania", "Slovakia",
         "Slovenia", "Spain", "Sweden",
         "AUSTRIA", "BELGIUM", "BULGARIA", "CROATIA", "CYPRUS", "CZECH REPUBLIC", "DENMARK",
         "ESTONIA", "FINLAND", "FRANCE", "GERMANY", "GREECE", "HUNGARY", "IRELAND", "ITALY", "LATVIA",
         "LITHUANIA", "LUXEMBOURG", "MALTA", "NETHERLANDS", "POLAND", "PORTUGAL", "ROMANIA", "SLOVAKIA",
         "SLOVENIA", "SPAIN", "SWEDEN"]


def is_eu_country(flight_data: pd.DataFrame, country_col: str) -> pd.DataFrame:
    """
    Check if the specific country in the "STATE_NAME" cloumn is part of the EU-27.
    
    :param flight_data: DataFrame containing flight information
    :type flight_data: Dataframe with only flights from EU countries
    :return: Description
    :rtype: DataFrame
    """
    # Add a new column to indicate if the country is in the EU-27. 
    # Then filter the DataFrame based on this column.
    flight_data["IS_EU_COUNTRY"] = flight_data[country_col].apply(lambda x: x in eu_27)
    flight_data = flight_data.drop(flight_data[flight_data["IS_EU_COUNTRY"] == False].index)

    # Delete the helper column
    flight_data = flight_data.drop(columns=["IS_EU_COUNTRY"], axis=1)

    # Return the filtered DataFrame
    return flight_data


def get_airport_traffic(start_year: int, end_year: int):
    """
    Work on the entire airport traffic data from 2016 to 2025 and filter for EU-27 countries only.
    Combine all years into one DataFrame and save as a CSV file.

    :return: None
    :rtype: None    
    """

    tot_result_df = pd.DataFrame({
        "YEAR": [],
        "MONTH": [],
        "STATE_NAME": [],
        "FLT_DEP_1": [],
        "FLT_ARR_1": [],
        "FLT_TOT_1": [],
        })

    for i in range(start_year, end_year+1):
        airport_traffic = (pd.read_csv(f"Data/airport-traffic-data/airport_traffic_{i}.csv")
                           .drop(columns=['FLT_DATE', 'MONTH_MON', 'APT_ICAO', 'APT_NAME', 'FLT_DEP_IFR_2', 'FLT_ARR_IFR_2', 'FLT_TOT_IFR_2'], axis=1)
                           .rename(columns={'MONTH_NUM': 'MONTH'})
                        )
        result_df = is_eu_country(airport_traffic, "STATE_NAME").dropna()
        result_df = result_df.groupby(['YEAR', 'MONTH','STATE_NAME'], as_index=False).sum()
        tot_result_df = pd.concat([tot_result_df, result_df], ignore_index=True)
    
    # create one big dataframe with all the years data.
    tot_result_df = tot_result_df.astype({'YEAR': int, 'MONTH': int, 'FLT_DEP_1': int, 'FLT_ARR_1': int, 'FLT_TOT_1': int})
    tot_result_df.to_csv(f"Data/airport-traffic-data/cleaned-data/eu_airport_traffic_{start_year}_{end_year}.csv", index=False)


def calculate_total_fuel_consumption(emmission_dataframe : pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the total fuel consumption based on CO2 emissions.

    :param emmission_dataframe: DataFrame containing CO2 emissions data
    :type emmission_dataframe: pd.DataFrame
    :return: DataFrame with an additional column for total fuel consumption
    :rtype: pd.DataFrame
    """
    # Assuming a conversion factor from CO2 emissions to fuel consumption
    # This number is based on stoichiometric combustion chemistrt 
    # and comes from established IPCC/ICAO references (e.g., IPCC 2006 Guidelines for National Greenhouse Gas Inventories).
    CO2_TO_FUEL_CONVERSION_FACTOR = 3.16  # Example factor, actual value may vary

    emmission_dataframe['FUEL_CONSUMPTION_TONNES'] = emmission_dataframe['CO2_QTY_TONNES'] / CO2_TO_FUEL_CONVERSION_FACTOR
    return emmission_dataframe

def clean_emissions_data(start_year: int, end_year: int):
    """
    Work on the entire CO2 emissions data from 2016 to 2021 and filter for EU-27 countries only.
    Combine all years into one DataFrame and save as a CSV file.

    :param start_year: The starting year for processing CO2 emissions data
    :type start_year: int
    :param end_year: The ending year for processing CO2 emissions data
    :type end_year: int
    :rtype: None    
    """
    tot_result_df = pd.DataFrame({
        "YEAR": [],
        "MONTH": [],
        "STATE_NAME": [],
        "CO2_QTY_TONNES": [],
        "COUNTRY_TRAFFIC": []
        })

    for i in range(start_year, end_year+1):
        co2_data = pd.read_csv(f"Data/state_co2_data/co2_emmissions_by_state_{i}.csv").drop(columns=['STATE_CODE', 'NOTE'], axis=1).rename(columns={'TF': 'COUNTRY_TRAFFIC'})
        
        # Check if there is a day column and drop it
        if 'FLIGHT_MONTH' in co2_data.columns:
            co2_data = co2_data.drop(columns=['FLIGHT_MONTH'], axis=1)
        
        result_df = is_eu_country(co2_data, "STATE_NAME").dropna()
        tot_result_df = pd.concat([tot_result_df, result_df], ignore_index=True)
    
    # Calculate total fuel consumption
    tot_result_df = calculate_total_fuel_consumption(tot_result_df).round({'FUEL_CONSUMPTION_TONNES': 2})
    
    # create one big dataframe with all the years data.
    tot_result_df = tot_result_df.round({'CO2_QTY_TONNES': 2})
    tot_result_df['YEAR'] = tot_result_df['YEAR'].astype(int)
    tot_result_df['MONTH'] = tot_result_df['MONTH'].astype(int)
    tot_result_df["COUNTRY_TRAFFIC"] = tot_result_df["COUNTRY_TRAFFIC"].astype(int)
    tot_result_df.to_csv(f"Data/state_co2_data/cleaned_data/eu_co2_emmissions_{start_year}_{end_year}.csv", index=False)

if __name__ == "__main__":

    # test = pd.read_csv("Data/state_co2_data/co2_emmissions_by_state_2010.csv").drop(columns=['STATE_CODE', 'NOTE'], axis=1)
    # new_data = test.groupby(['YEAR', 'MONTH', 'STATE_NAME'], as_index=False).sum()

    # get_airport_traffic(2016, 2025)
    clean_emissions_data(2010, 2025)
    