import pandas as pd
import airport_flight as af

def load_population_data(file_path: str) -> pd.DataFrame:
    """
    Load population data from a CSV file.

    :param file_path: Path to the CSV file containing population data
    :type file_path: str
    :return: DataFrame containing population data
    :rtype: pd.DataFrame
    """
    population_data = pd.read_csv(file_path)
    return population_data

def clean_single_col_data(data: pd.DataFrame, separator: str, col_to_sep: int) -> pd.DataFrame:
    """
    Get the data column where there are multiple values in a single row and clean it by breaking it apart.
    """
    columns = ["Countries"] + data.columns.tolist()[col_to_sep].split(separator)
    length_of_dataframe = len(data)
    new_data_dict = {}

    # Iterate through each row and split the values into separate columns
    for i in range(length_of_dataframe):
        row_values = data.iloc[i, col_to_sep].split(separator)
        for j, col in enumerate(columns):
            if col not in new_data_dict:
                new_data_dict[col] = []
            new_data_dict[col].append(row_values[j])
    
    # Return a new cleaned DataFrame
    cleaned_data = pd.DataFrame(new_data_dict)
    return cleaned_data

def check_eu_countries(pop_data: pd.DataFrame, country_col: str) -> pd.DataFrame:
    """
    Call the is_eu_country function from airport_flight.py to filter population data for EU 27 countries.

    :param pop_data: DataFrame containing population data
    :type pop_data: pd.DataFrame
    :param country_col: Name of the column containing country names
    :type country_col: str

    :return: Filtered DataFrame with only EU 27 countries
    """
    filtered_data = af.is_eu_country(pop_data, country_col)
    return filtered_data

def calculate_population_growth(data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate year-over-year population growth rates for years 2012-2023.

    For each consecutive year pair, calculates the growth rate using:
    (pop_year2 - pop_year1) / pop_year1

    :param data: DataFrame with population columns for years 2012-2023
    :type data: pd.DataFrame
    :return: DataFrame with original data plus population change columns
    :rtype: pd.DataFrame
    """
    
    # Create a copy to avoid modifying the original dataframe
    result_df = data.copy()

    # Define the years we're working with
    # result_df.columns.to_list().remove("Countries")
    years = list(range(2012, 2024))  # 2012 to 2023 inclusive

    # Calculate population change for each consecutive year pair
    for i in range(len(years) - 1):
        year1 = years[i]
        year2 = years[i + 1]

        # Column names for the population data
        pop_col1 = str(year1)
        pop_col2 = str(year2)

        # New column name for the population change
        change_col = f"pop_change_{year1}-{year2}"

        # Calculate the population change rate
        # Formula: (pop_year2 - pop_year1) / pop_year1
        result_df[change_col] = (
            pd.to_numeric(result_df[pop_col2], errors='coerce') -
            pd.to_numeric(result_df[pop_col1], errors='coerce')
        ) / pd.to_numeric(result_df[pop_col1], errors='coerce')

    return result_df


if __name__ == "__main__":
    data_path = "Data/population-data/xls0009913_i.csv"
    population_df = load_population_data(data_path)
    cleaned_pop = check_eu_countries(clean_single_col_data(population_df, separator=";", col_to_sep=0), country_col="Countries")
    new_data = calculate_population_growth(cleaned_pop)
    new_data.to_csv("Data/population-data/eu_population_growth_2012_2023.csv", index=False)