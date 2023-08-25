from datetime import datetime
import logging

from sqlalchemy.engine import create_engine, Engine


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Creates and returns logger object.
    :param name: Name of the logger.
    :param level: Set the root logger level to the specified level.
    :return: logger object
    """
    logs_path = '/Users/satish.chitturi/CodeBase/Corpository-OnDemand-Scoring-Logs/'
    file_name = logs_path + f'on_demand_log_{datetime.now().strftime("%Y%m%d%H%M%S")}.log'

    if name.strip() is not None and name.strip() != '' and level in [50, 40, 30, 20, 10, 0]:
        logging.basicConfig(
            filename=file_name,
            level=level,  # Adjust the logging level as needed
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        logger = logging.getLogger(name)
        logger.setLevel(level)
        return logger
    else:
        raise ValueError(f"Invalid logger name or log level. logger name: '{name}', Log level: {level}")


def generate_query(table_name: str, select_columns: list, filter_columns: dict, filter_values: dict) -> str:
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

    get_logger('Utils-Generate Query', logging.INFO).info(f"Query Generated: '{query}'")

    return query


class ConnectionUtils:
    def __init__(self):
        self.info_logger = logging.getLogger('info_logger')
        self.info_logger.setLevel('INFO')

        self.db_username = "yubireaduser"
        self.db_password = ""
        self.db_port = 3306

        self.etl_host = "yubi-replica-corpository-etl-data-db.cxnf9ffsrpfk.ap-south-1.rds.amazonaws.com"
        self.live_host = "yubi-replica-corpository-db.cxnf9ffsrpfk.ap-south-1.rds.amazonaws.com"

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
                    etl_crawler_ext_connection_url = f"mysql://{self.db_username}:{self.db_password}@{self.etl_host}:{self.db_port}/crawler_ext"
                    return create_engine(etl_crawler_ext_connection_url)
                elif connection_name == 'etl_etl_data':
                    etl_etl_data_connection_url = f"mysql://{self.db_username}:{self.db_password}@{self.etl_host}:{self.db_port}/etl_data"
                    return create_engine(etl_etl_data_connection_url)
                elif connection_name == 'live_crawler_output':
                    live_crawler_output_connection_url = f"mysql://{self.db_username}:{self.db_password}@{self.live_host}:{self.db_port}/crawler_output"
                    return create_engine(live_crawler_output_connection_url)
            else:
                return globals()['connection']
        else:
            self.error_logger.error(f"Invalid connection name: '{connection_name}'. Please check!")
            raise ValueError(f"Invalid connection name: '{connection_name}'. Please check!")


