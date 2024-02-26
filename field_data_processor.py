import logging
from data_ingestion import create_db_engine, query_data, read_from_web_CSV


class FieldDataProcessor:
    """
    A class for processing field data.

    This class provides methods to ingest data from a SQL database, perform data transformations,
    and merge weather station mapping data with the main dataset.

    Attributes:
    - db_path (str): The file path or database URL for the SQL database.
    - sql_query (str): The SQL query to retrieve data from the database.
    - columns_to_rename (dict): A dictionary mapping column names to be renamed.
    - values_to_rename (dict): A dictionary mapping values in the dataset to be renamed.
    - weather_map_data (str): The URL of the CSV file containing weather mapping data.

    Methods:
    - initialize_logging(logging_level): Sets up logging for the class instance.
    - ingest_sql_data(): Loads data from the SQL database and returns it as a DataFrame.
    - rename_columns(): Renames specified columns in the DataFrame.
    - apply_corrections(column_name='Crop_type', abs_column='Elevation'): Applies corrections to specified columns in the DataFrame.
    - weather_station_mapping(): Merges weather station mapping data with the main DataFrame.
    - process(): Executes the data processing pipeline by calling the ingest, rename, correction, and mapping methods sequentially.
    """

    def __init__(self, config_params, logging_level="INFO"):
        """
        Initialize the FieldDataProcessor class.

        Parameters:
        - config_params (dict): A dictionary containing configuration parameters including
          'db_path', 'sql_query', 'columns_to_rename', 'values_to_rename', and 'weather_mapping_csv'.
        - logging_level (str): The desired logging level for the class instance. Default is "INFO".
        """
        self.db_path = config_params["db_path"]
        self.sql_query = config_params["sql_query"]
        self.columns_to_rename = config_params["columns_to_rename"]
        self.values_to_rename = config_params["values_to_rename"]
        self.weather_map_data = config_params["weather_mapping_csv"]

        # Add the rest of your class code here
        self.initialize_logging(logging_level)

        # We create empty objects to store the DataFrame and engine in
        self.df = None
        self.engine = None

    def initialize_logging(self, logging_level):
        """
        Set up logging for the FieldDataProcessor instance.

        Parameters:
        - logging_level (str): The desired logging level.
        """
        logger_name = __name__ + ".FieldDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = (
            False  # Prevents log messages from being propagated to the root logger
        )

        # Set logging level
        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":  # Option to disable logging
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO  # Default to INFO

        self.logger.setLevel(log_level)

        # Only add handler if not already added to avoid duplicate messages
        if not self.logger.handlers:
            ch = logging.StreamHandler()  # Create console handler
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def ingest_sql_data(self):
        """
        Load data from SQL database and return as DataFrame.

        Returns:
        - df: DataFrame containing the loaded data.
        """
        try:
            self.engine = create_db_engine(self.db_path)
            self.df = query_data(self.engine, self.sql_query)
            self.logger.info("Successfully loaded data.")
            return self.df
        except Exception as e:
            self.logger.error(f"Failed to load data from SQL database. Error: {e}")
            raise

    def rename_columns(self):
        """
        Rename specified columns in the DataFrame.
        """
        # Extract the columns to rename from the configuration
        column1, column2 = (
            list(self.columns_to_rename.keys())[0],
            list(self.columns_to_rename.values())[0],
        )

        # Temporarily rename one of the columns to avoid a naming conflict
        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"

        # Perform the swap
        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})

        self.logger.info(f"Swapped columns: {column1} with {column2}")

    def apply_corrections(self, column_name="Crop_type", abs_column="Elevation"):
        """
        Apply corrections to specified columns in the DataFrame.

        Parameters:
        - column_name (str): The name of the column to apply corrections to. Default is 'Crop_type'.
        - abs_column (str): The name of the column for which absolute values will be taken. Default is 'Elevation'.
        """
        self.df[abs_column] = self.df[abs_column].abs()
        self.df[column_name] = self.df[column_name].apply(
            lambda crop: self.values_to_rename.get(crop, crop)
        )
        self.df[column_name] = self.df[column_name].str.strip()

    def weather_station_mapping(self):
        """
        Merge weather station mapping data with the main DataFrame.

        Returns:
        - df: DataFrame containing the merged data.
        """
        self.df = self.df.merge(
            read_from_web_CSV(self.weather_map_data), on="Field_ID", how="left"
        )
        return self.df

    def process(self):
        """
        Execute the data processing pipeline.

        This method sequentially calls the ingest, rename, correction, and mapping methods.
        """
        self.ingest_sql_data()
        # Insert your code here
        self.rename_columns()
        self.apply_corrections()
        self.weather_station_mapping()
        self.df = self.df.drop(columns="Unnamed: 0")
