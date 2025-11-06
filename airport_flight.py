import pandas as pd

eu_27 = ["Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czech Republic", "Denmark",
         "Estonia", "Finland", "France", "Germany", "Greece", "Hungary", "Ireland", "Italy", "Latvia",
         "Lithuania", "Luxembourg", "Malta", "Netherlands", "Poland", "Portugal", "Romania", "Slovakia",
         "Slovenia", "Spain", "Sweden"]

def is_eu_country(flight_data: pd.DataFrame) -> pd.DataFrame:
    """
    Check if the specific country in the "STATE_NAME" cloumn is part of the EU-27.
    
    :param flight_data: DataFrame containing flight information
    :type flight_data: Dataframe with only flights from EU countries
    :return: Description
    :rtype: DataFrame
    """

    flight_data["IS_EU_COUNTRY"] = flight_data["STATE_NAME"].apply(lambda x: x in eu_27)
    flight_data = flight_data.drop(flight_data[flight_data["IS_EU_COUNTRY"] == False].index)
    return flight_data


if __name__ == "__main__":
    tot_result_df = pd.DataFrame({
        "FLT_DATE": [],
        "APT_ICAO": [],
        "STATE_NAME": [],
        "FLT_DEP_1": [],
        "FLT_ARR_1": [],
        "FLT_TOT_1": [],
    })
    for i in range(2016, 2025):
        airport_traffic = pd.read_csv(f"Data/airport-traffic-data/airport_traffic_{i}.csv")
        df = pd.DataFrame(airport_traffic)
        result_df = is_eu_country(df)
        tot_result_df = pd.concat([tot_result_df, result_df], ignore_index=True)
        print(result_df.head(5))
    
    # create one big dataframe with all the years data.
    tot_result_df = tot_result_df.drop(columns=["IS_EU_COUNTRY"], axis=1)
    tot_result_df.to_csv(f"Data/airport-traffic-data/cleaned-data/eu_airport_traffic_2016_2024.csv", index=False)