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
    return flight_data


if __name__ == "__main__":
    data = {
        "STATE_NAME": ["France", "Brazil", "Germany", "Canada", "Italy", "Japan"]
    }
    df = pd.DataFrame(data)
    result_df = is_eu_country(df)
    print(result_df)