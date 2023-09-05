import logging
import os
from configparser import ConfigParser
from datetime import datetime

from sqlalchemy.engine import create_engine, Engine

config = ConfigParser()
config.read(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.ini'))


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Creates and returns logger object.
    :param name: Name of the logger.
    :param level: Set the root logger level to the specified level.
    :return: logger object
    """
    logs_path = config.get('Logs', 'logs_path')
    file_name = logs_path + f'on_demand_log_{datetime.now().strftime("%Y%m%d%H%M%S")}.log'

    if name.strip() is not None and name.strip() != '' and level in [50, 40, 30, 20, 10, 0]:
        logging.basicConfig(
            filename=file_name,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        logger = logging.getLogger(name)
        logger.setLevel(level)
        return logger
    else:
        raise ValueError(f"Invalid logger name or log level. logger name: '{name}', Log level: {level}")


def generate_query(table_name: str, select_columns: list, filter_columns: dict = None, filter_values: dict = None) -> str:
    """
    This function generates SQL query and returns the query string.
    :param table_name: nam eof table from where data should be fetched.
    :param select_columns: list of columns that needs to be selected from table.
    :param filter_columns: column used to filter the rows from table.
    :param filter_values: dict with keys as column_names and value as list of values that is used to filter the table.
                          {'company_master_id':[761, 762, 763, ..], 'financial_year': ['2019-2020', '2020-2021', ..]}
    :return: query string.
    """
    # Check if columns mentioned in cols list have their respective filter values.
    if set(filter_columns.keys()) != filter_values.keys():
        error = f"Filter values list for some/all column are missing. Columns: {str(filter_columns)}, filter values dict: {str(filter_values)}"
        get_logger('Utils-Generate Query', logging.INFO).info(f"Error: '{error}'")
        raise ValueError(error)

    # Construct the SELECT part of the query
    select_clause = ', '.join(select_columns)

    if filter_columns:
        # Construct the WHERE condition
        filters = set()
        for column_name in filter_columns.keys():
            if filter_columns[column_name].strip() in ['str', 'date']:
                formatted_vals = [f"'{val}'" for val in filter_values[column_name]]
                filters.add(f"{column_name} IN ({', '.join(map(str, formatted_vals))})")
            else:
                filters.add(f"{column_name} IN ({', '.join(map(str, filter_values[column_name]))})")

        where_condition = ' AND '.join(map(str, filters))
        # Construct the final query
        query = f"SELECT {select_clause} FROM {table_name} WHERE {where_condition};"
    else:
        query = f"SELECT {select_clause} FROM {table_name};"

    get_logger('utils', logging.INFO).info(f"Query Generated: '{query}'")

    return query


def get_indian_financial_year(date):
    year = date.year
    if date.month >= 4:  # If the date is in April or later
        start_date = datetime(year, 4, 1).date()
        end_date = datetime(year + 1, 3, 31).date()
    else:  # If the date is before April
        start_date = datetime(year - 1, 4, 1).date()
        end_date = datetime(year, 3, 31).date()
    return start_date, end_date


def get_previous_indian_financial_year(date):
    year = date.year - 1
    if date.month >= 4:  # If the date is in April or later
        start_date = datetime(year, 4, 1).date()
        end_date = datetime(year + 1, 3, 31).date()
    else:  # If the date is before April
        start_date = datetime(year - 1, 4, 1).date()
        end_date = datetime(year, 3, 31).date()
    return start_date, end_date


class Utils:
    def __init__(self):
        self.logger = get_logger('utils', logging.INFO)
        self.logger.setLevel('INFO')

        self.db_username = config.get('Database.Credentials', 'db_username')
        self.db_password = config.get('Database.Credentials', 'db_password')
        self.db_port = config.get('Database.Credentials', 'db_port')

        self.etl_host = config.get('Database.Hosts', 'etl_db_host')
        self.live_host = config.get('Database.Hosts', 'live_db_host')

        self.crawler_ext_schema = config.get('Database.Schemas', 'crawler_ext_schema')
        self.etl_data_schema = config.get('Database.Schemas', 'etl_data_schema')
        self.crawler_output_schema = config.get('Database.Schemas', 'crawler_output_schema')

    def get_db_connection(self, connection_name: str) -> Engine:
        """
        This function creates a connection to Corpository's MariaDB and returns it. If there is an existing connection, this
        function returns the same. Connection for 'ETL DB - crawler_ext', 'ETL DB - etl_data',  'Live DB - crawler_output'
        schema's can be created.

        :param connection_name: name of the connection that is used to create connection to required schema.
        :return: Engine
        """
        if connection_name and connection_name in ['etl_crawler_ext', 'etl_etl_data', 'live_crawler_output']:
            if connection_name + '_connection' not in globals():
                if connection_name == 'etl_crawler_ext':
                    etl_crawler_ext_connection_url = f"mysql://{self.db_username}:{self.db_password}@{self.etl_host}:{self.db_port}/{self.crawler_ext_schema}"
                    return create_engine(etl_crawler_ext_connection_url)
                elif connection_name == 'etl_etl_data':
                    etl_etl_data_connection_url = f"mysql://{self.db_username}:{self.db_password}@{self.etl_host}:{self.db_port}/{self.etl_data_schema}"
                    return create_engine(etl_etl_data_connection_url)
                elif connection_name == 'live_crawler_output':
                    live_crawler_output_connection_url = f"mysql://{self.db_username}:{self.db_password}@{self.live_host}:{self.db_port}/{self.crawler_output_schema}"
                    return create_engine(live_crawler_output_connection_url)
            else:
                return globals()['connection']
        else:
            get_logger('Utils DB Connection', logging.INFO).error(f"Invalid connection name: '{connection_name}'. Please check!")
            raise ValueError(f"Invalid connection name: '{connection_name}'. Please check!")
