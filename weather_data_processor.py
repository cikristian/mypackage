import re # Importing the regex pattern
import numpy as np


patterns = {
    'Rainfall': r'(\d+(\.\d+)?)\s?mm',
     'Temperature': r'(\d+(\.\d+)?)\s?C',
    'Pollution_level': r'=\s*(-?\d+(\.\d+)?)|Pollution at \s*(-?\d+(\.\d+)?)'
    }

def extract_measurement(message):
    """
    Extracts a numeric measurement value from a given message string.

    The function applies regular expressions to identify and extract
    numeric values related to different types of measurements such as
    Rainfall, Average Temperatures, and Pollution Levels from a text message.
    It returns the key of the matching record, and first matching value as a floating-point number.
    
    Parameters:
    message (str): A string message containing the measurement information.

    Returns:
    float: The extracted numeric value of the measurement if a match is found;
           otherwise, None.

    The function uses the following patterns for extraction:
    - Rainfall: Matches numbers (including decimal) followed by 'mm', optionally spaced.
    - Ave_temps: Matches numbers (including decimal) followed by 'C', optionally spaced.
    - Pollution_level: Matches numbers (including decimal) following 'Pollution at' or '='.
    
    Example usage:
    extract_measurement("【2022-01-04 21:47:48】温度感应: 现在温度是 12.82C.")
    # Returns: 'Temperature', 12.82
    """
    
    for key, pattern in patterns.items(): # Loop through all of the patterns and check if it matches the pattern value.
        match = re.search(pattern, message)
        if match:
            # Extract the first group that matches, which should be the measurement value if all previous matches are empty.
            # print(match.groups()) # Uncomment this line to help you debug your regex patterns.
            return key, float(next((x for x in match.groups() if x is not None)))
    
    return None, None

# The function creates a tuple with the measurement type and value into a Pandas Series
result = weather_station_df['Message'].apply(extract_measurement)

# Create separate columns for 'Measurement' and 'extracted_value' by unpacking the tuple with Lambda functions.
weather_station_df['Measurement'] = result.apply(lambda x: x[0])
weather_station_df['Value'] = result.apply(lambda x: x[1])

