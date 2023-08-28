#  Author : Naga Satish Chitturi(CA0316)
#  Created : 2023-08-20
#  Last Modified : 2020-02-23
#  Description : This class contains related to auditor category features(KPIs).
#
#  Change Log
#  ---------------------------------------------------------------------------
#   Date			Author			Comment
#  ---------------------------------------------------------------------------
#

import logging
from datetime import datetime, timedelta
from itertools import product

import numpy as np
import pandas as pd

from classes import Utils, get_logger, generate_query

utils = Utils()


class DirectorsFeatures:
    """
    Class to generate Director Features
    """

    def __init__(self, companies_list: list, as_on_date: datetime.date):
        self.logger = get_logger('DirectorLogger', logging.INFO)
        self.companies = companies_list
        self.as_on_date = as_on_date

        self.end_year = self.as_on_date.year - 1 if self.as_on_date.month <= 3 else self.as_on_date.year
        self.start_year = self.end_year - 4
        as_on_dt_fy = f"{self.as_on_date.year - 2}-{self.as_on_date.year - 1}" if self.as_on_date.month <= 3 else f"{self.as_on_date.year - 1}-{self.as_on_date.year}"

        self.as_on_dt_fy = as_on_dt_fy
        self. valid_years = [f'{i}-{i + 1}' for i in range(self.start_year, self.end_year)]

        self.live_crawler_output_connection = utils.get_db_connection('live_crawler_output')
        self.etl_etl_data_connection = utils.get_db_connection('etl_etl_data')
        # self.etl_crawler_ext_connection = utils.get_db_connection('etl_crawler_ext')

    def resigned_director_feat(self):
        self.logger.info("Extracting resigned directors features.")
        director_history_details_qry = generate_query(
            table_name='director_history_details',
            select_columns=['company_master_id', 'director_master_id', 'designation', 'appointment_original_date',
                            'date_cessation'],
            filter_columns={'company_master_id': 'int'},
            filter_values={'company_master_id': self.companies}
        )

        director_history_details = pd.read_sql(director_history_details_qry, self.live_crawler_output_connection)

        director_history_details['appointment_original_date'] = director_history_details['appointment_original_date'].str.strip()
        director_history_details['date_cessation'] = director_history_details['date_cessation'].str.strip()

        director_history_details['appointment_original_date'] = director_history_details['appointment_original_date'].replace('-', pd.NaT)
        director_history_details['date_cessation'] = director_history_details['date_cessation'].replace('-', pd.NaT)

        director_history_details['appointment_original_date'] = pd.to_datetime(director_history_details['appointment_original_date'], format='%d/%m/%Y')
        director_history_details['date_cessation'] = pd.to_datetime(director_history_details['date_cessation'], format='%d/%m/%Y')

        director_history_details = director_history_details[
            (director_history_details['date_cessation'] >= (pd.to_datetime(self.as_on_date) - timedelta(days=730))) &
            (director_history_details['date_cessation'] <= pd.to_datetime(self.as_on_date))
        ].drop_duplicates()
        ################################################################################################################
        director_master_ids = list(set(director_history_details['director_master_id'].tolist()))
        director_master_qry = generate_query(
            table_name='director_master',
            select_columns=['director_master_id', 'DIN'],
            filter_columns={'director_master_id': 'int'},
            filter_values={'director_master_id': director_master_ids}
        )
        director_master = pd.read_sql(director_master_qry, self.live_crawler_output_connection)
        director_master.rename(columns={'DIN': 'din'}, inplace=True)
        ################################################################################################################
        director_history_details = pd.merge(director_history_details, director_master, on='director_master_id',
                                            how='inner').drop(columns=['director_master_id'])
        ################################################################################################################
        dins = list(set((director_history_details['din'].tolist())))
        dir12_director_dtls_qry = generate_query(
            table_name='dir12_director_dtls',
            select_columns=['company_master_id', 'director_din', 't_dir_category', 'appt_chng_desig_date'],
            # filter_columns={'company_master_id': 'int', 'director_din': 'str'},
            # filter_values={'company_master_id': self.companies, 'director_din': dins}
            filter_columns={'company_master_id': 'int'},
            filter_values={'company_master_id': self.companies}
        )

        dir12_director_dtls = pd.read_sql(dir12_director_dtls_qry, self.etl_etl_data_connection)
        dir12_director_dtls['t_dir_category'] = dir12_director_dtls['t_dir_category'].str.strip().str.lower()
        dir12_director_dtls['director_din'] = pd.to_numeric(dir12_director_dtls['director_din'], errors='coerce')

        dir12_director_dtls = dir12_director_dtls[
            dir12_director_dtls['director_din'].notnull()
        ]

        dir12_director_dtls['director_din'] = dir12_director_dtls['director_din'].astype(int)

        dir12_director_dtls.rename(columns={'director_din': 'din'}, inplace=True)
        dir12_director_dtls = dir12_director_dtls.drop_duplicates()

        window = dir12_director_dtls.groupby(['company_master_id', 'din'])[
            'appt_chng_desig_date'].rank(method='first', ascending=False)

        dir12_director_dtls['row'] = window

        dir12_director_dtls = dir12_director_dtls[dir12_director_dtls['row'] == 1]
        dir12_director_dtls = dir12_director_dtls.drop(columns=['row'])

        dir_resign_dtls = pd.merge(director_history_details, dir12_director_dtls, on=['company_master_id', 'din'],
                                   how='left')

        dir_resign_dtls['din'] = dir_resign_dtls['din'].astype(str)
        ################################################################################################################
        grouped_df = dir_resign_dtls.groupby(['company_master_id', 'date_cessation'])

        # Calculate the new columns
        dir_resign_dtls_by_day = grouped_df.apply(lambda x: pd.Series({
            'num_of_ind_dir_resigns': x.loc[x['t_dir_category'] == 'independent', 'din'].nunique(),
            'num_of_pro_dir_resigns': x.loc[x['t_dir_category'] == 'promoter', 'din'].nunique(),
            'num_of_dir_resigns': x['din'].nunique()
        }))
        dir_resign_dtls_by_day = dir_resign_dtls_by_day.reset_index()

        _days = lambda i: pd.DateOffset(days=i)

        window_for_last_1_year = _days(364)
        window_for_last_2_year = _days(729)

        # Calculate the rolling sum for the specified windows
        dir_resign_dtls_by_day['n_indep_dir_resigned_in_1_yr'] = dir_resign_dtls_by_day.groupby('company_master_id')[
            'num_of_ind_dir_resigns'].rolling(window=window_for_last_1_year).sum()

        dir_resign_dtls_by_day['n_indep_dir_resigned_in_2_yr'] = dir_resign_dtls_by_day.groupby('company_master_id')[
            'num_of_ind_dir_resigns'].rolling(window=window_for_last_2_year).sum()

        dir_resign_dtls_by_day['n_promoter_dir_resigned_in_1_yr'] = dir_resign_dtls_by_day.groupby('company_master_id')[
            'num_of_pro_dir_resigns'].rolling(window=window_for_last_1_year).sum()

        dir_resign_dtls_by_day['n_promoter_dir_resigned_in_2_yr'] = dir_resign_dtls_by_day.groupby('company_master_id')[
            'num_of_pro_dir_resigns'].rolling(window=window_for_last_2_year).sum()

        dir_resign_dtls_by_day['n_dir_resigned_in_1_yr'] = dir_resign_dtls_by_day.groupby('company_master_id')[
            'num_of_dir_resigns'].rolling(window=window_for_last_1_year).sum()

        dir_resign_dtls_by_day['n_dir_resigned_in_2_yr'] = dir_resign_dtls_by_day.groupby('company_master_id')[
            'num_of_dir_resigns'].rolling(window=window_for_last_2_year).sum()

        self.logger.info("Completed Extracting resigned directors features.")
        return dir_resign_dtls_by_day

