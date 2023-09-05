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

import pandas as pd

from classes import Utils, get_logger, generate_query
from classes.Utils import get_indian_financial_year

utils = Utils()


class DirectorsFeatures:
    """
    Class to generate Director Features
    """

    def __init__(self, companies_list: list, as_on_date: datetime.date):
        self.logger = get_logger('DirectorLogger', logging.INFO)
        self.companies = companies_list
        self.as_on_date = pd.to_datetime(as_on_date)

        self.end_year = self.as_on_date.year - 1 if self.as_on_date.month <= 3 else self.as_on_date.year
        self.start_year = self.end_year - 4

        if self.as_on_date.month <= 3:
            as_on_dt_fy = f"{self.as_on_date.year - 2}-{self.as_on_date.year - 1}"
        else:
            as_on_dt_fy = f"{self.as_on_date.year - 1}-{self.as_on_date.year}"

        self.as_on_dt_fy = as_on_dt_fy
        self. valid_years = [f'{i}-{i + 1}' for i in range(self.start_year, self.end_year)]

        self.live_crawler_output_connection = utils.get_db_connection('live_crawler_output')
        self.etl_etl_data_connection = utils.get_db_connection('etl_etl_data')
        # self.etl_crawler_ext_connection = utils.get_db_connection('etl_crawler_ext')

        self.logger.info("querying director_history_details table.")
        director_history_details_qry = generate_query(
            table_name='director_history_details',
            select_columns=['company_master_id', 'director_master_id', 'designation', 'appointment_original_date',
                            'date_cessation'],
            filter_columns={'company_master_id': 'int'},
            filter_values={'company_master_id': self.companies}
        )
        director_history_details = pd.read_sql(director_history_details_qry, self.live_crawler_output_connection)
        self.logger.info("completed querying director_history_details table.")

        director_history_details['designation'] = director_history_details['designation'].str.strip().str.lower()

        director_history_details['appointment_original_date'] = pd.to_datetime(
            director_history_details['appointment_original_date'], format='%d/%m/%Y', errors='coerce')
        director_history_details['date_cessation'] = pd.to_datetime(director_history_details['date_cessation'],
                                                                    format='%d/%m/%Y', errors='coerce')

        self.director_history_details_tbl = director_history_details

        self.logger.info("querying director_master table.")
        director_master_ids = list(set(director_history_details['director_master_id'].tolist()))
        director_master_qry = generate_query(
            table_name='director_master',
            select_columns=['director_master_id', 'DIN'],
            filter_columns={'director_master_id': 'int'},
            filter_values={'director_master_id': director_master_ids}
        )
        director_master = pd.read_sql(director_master_qry, self.live_crawler_output_connection)
        director_master.rename(columns={'DIN': 'din'}, inplace=True)
        self.logger.info("completed querying director_master table.")

        self.director_master_tbl = director_master

    def resigned_director_feat(self):
        self.logger.info("Extracting resigned directors features.")

        director_history_details = self.director_history_details_tbl.copy()
        director_history_details = director_history_details[
            director_history_details['date_cessation'].between(self.as_on_date - timedelta(days=730), self.as_on_date)
        ].drop_duplicates()
        ################################################################################################################
        if director_history_details.size > 0:
            director_history_details = pd.merge(
                director_history_details, self.director_master_tbl, on='director_master_id', how='inner'
            ).drop(columns=['director_master_id'])
            ############################################################################################################
            # dins = list(set((director_history_details['din'].tolist())))
            self.logger.info("querying dir12_director_dtls table.")

            dir12_director_dtls_qry = generate_query(
                table_name='dir12_director_dtls',
                select_columns=['company_master_id', 'director_din', 't_dir_category', 'appt_chng_desig_date'],
                # filter_columns={'company_master_id': 'int', 'director_din': 'str'},
                # filter_values={'company_master_id': self.companies, 'director_din': dins}
                filter_columns={'company_master_id': 'int'},
                filter_values={'company_master_id': self.companies}
            )

            dir12_director_dtls = pd.read_sql(dir12_director_dtls_qry, self.etl_etl_data_connection)
            self.logger.info("completed querying dir12_director_dtls table.")

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
            self.logger.info("joined all the tables.")

            dir_resign_dtls['din'] = dir_resign_dtls['din'].astype(str)
            ############################################################################################################
            grouped_df = dir_resign_dtls.groupby(['company_master_id', 'date_cessation'])

            # Calculate the new columns
            dir_resign_dtls_by_day = grouped_df.apply(lambda x: pd.Series({
                'num_of_ind_dir_resigns': x.loc[x['t_dir_category'] == 'independent', 'din'].nunique(),
                'num_of_pro_dir_resigns': x.loc[x['t_dir_category'] == 'promoter', 'din'].nunique(),
                'num_of_dir_resigns': x['din'].nunique()
            }))

            dir_resign_dtls_by_day = dir_resign_dtls_by_day.reset_index()

            dir_resign_dtls_by_day["resigned_in_past_1_yr"] = dir_resign_dtls_by_day.apply(
                lambda row: 1 if (row['date_cessation'] >= self.as_on_date - timedelta(days=364)) else 0, axis=1
            )
            dir_resign_dtls_by_day["resigned_in_past_2_yr"] = dir_resign_dtls_by_day.apply(
                lambda row: 1 if (row['date_cessation'] >= self.as_on_date - timedelta(days=729)) else 0, axis=1
            )

            dir_resign_dtls_by_day_grouped = dir_resign_dtls_by_day.groupby(['company_master_id'])

            # Calculate the new columns
            result_df = dir_resign_dtls_by_day_grouped.apply(lambda x: pd.Series({
                'n_indep_dir_resigned_in_1_yr': x.loc[x['resigned_in_past_1_yr'] == 1, 'num_of_ind_dir_resigns'].sum(),
                'n_indep_dir_resigned_in_2_yr': x.loc[x['resigned_in_past_2_yr'] == 1, 'num_of_ind_dir_resigns'].sum(),
                'n_promoter_dir_resigned_in_1_yr': x.loc[x['resigned_in_past_1_yr'] == 1, 'num_of_pro_dir_resigns'].sum(),
                'n_promoter_dir_resigned_in_2_yr': x.loc[x['resigned_in_past_2_yr'] == 1, 'num_of_pro_dir_resigns'].sum(),
                'n_dir_resigned_in_1_yr': x.loc[x['resigned_in_past_1_yr'] == 1, 'num_of_dir_resigns'].sum(),
                'n_dir_resigned_in_2_yr': x.loc[x['resigned_in_past_2_yr'] == 1, 'num_of_dir_resigns'].sum(),
            }))
            result_df = result_df.reset_index()
            all_comp_df = pd.DataFrame(self.companies, columns=['company_master_id'])
            result_df = pd.merge(all_comp_df, result_df, on=['company_master_id'], how='left')
            result_df = result_df.fillna(value={
                "n_indep_dir_resigned_in_1_yr": 0,
                "n_indep_dir_resigned_in_2_yr": 0,
                "n_promoter_dir_resigned_in_1_yr": 0,
                "n_promoter_dir_resigned_in_2_yr": 0,
                'n_dir_resigned_in_1_yr': 0,
                'n_dir_resigned_in_2_yr': 0
                })
        else:
            result_df = pd.DataFrame(self.companies, columns=['company_master_id'])
            result_df['n_indep_dir_resigned_in_1_yr'] = 0
            result_df['n_indep_dir_resigned_in_2_yr'] = 0
            result_df['n_promoter_dir_resigned_in_1_yr'] = 0
            result_df['n_promoter_dir_resigned_in_2_yr'] = 0
            result_df['n_dir_resigned_in_1_yr'] = 0
            result_df['n_dir_resigned_in_2_yr'] = 0

        result_df['company_master_id'] = result_df['company_master_id'].astype('int64')
        result_df['n_indep_dir_resigned_in_1_yr'] = result_df['n_indep_dir_resigned_in_1_yr'].astype('int16')
        result_df['n_indep_dir_resigned_in_2_yr'] = result_df['n_indep_dir_resigned_in_2_yr'].astype('int16')
        result_df['n_promoter_dir_resigned_in_1_yr'] = result_df['n_promoter_dir_resigned_in_1_yr'].astype('int16')
        result_df['n_promoter_dir_resigned_in_2_yr'] = result_df['n_promoter_dir_resigned_in_2_yr'].astype('int16')
        result_df['n_dir_resigned_in_1_yr'] = result_df['n_dir_resigned_in_1_yr'].astype('int16')
        result_df['n_dir_resigned_in_2_yr'] = result_df['n_dir_resigned_in_2_yr'].astype('int16')

        self.logger.info("Completed Extracting resigned directors features.")
        return result_df

    def disq_dir_active(self):
        self.logger.info("Extracting disqualified directors active feature.")

        director_history_details = self.director_history_details_tbl.copy()

        if self.as_on_date.date() == pd.Timestamp.now().date():
            director_history_details['date_cessation'] = director_history_details.apply(
                lambda row: self.as_on_date if pd.isnull(row['date_cessation']) else None, axis=1)
        else:
            director_history_details['date_cessation'] = director_history_details.apply(
                lambda row: self.as_on_date if pd.isnull(row['date_cessation']) else (
                    self.as_on_date if row['date_cessation'] > self.as_on_date else row['date_cessation']
                ), axis=1)

        director_history_details = director_history_details[
                (director_history_details['appointment_original_date'] < director_history_details['date_cessation'])
            ]

        director_history_details = pd.merge(director_history_details, self.director_master_tbl,
                                            on=['director_master_id'], how='inner').drop(columns=['director_master_id'])
        ################################################################################################################
        dins = director_history_details['din'].tolist()
        self.logger.info("querying director_remarks table.")
        director_remarks_qry = generate_query(
            table_name='director_remarks',
            select_columns=['din', 'remarks'],
            filter_columns={'din': 'str'},
            filter_values={'din': dins}
        )
        director_remarks = pd.read_sql(director_remarks_qry, self.live_crawler_output_connection)
        self.logger.info("completed querying director_remarks table.")

        director_remarks['remarks'] = director_remarks['remarks'].str.strip().str.lower()
        director_remarks['din'] = director_remarks['din'].str.strip().astype(int)
        director_remarks = director_remarks[
            director_remarks['din'].notnull()
        ]

        pattern = r'from \d{2}/\d{2}/\d{2,4} to \d{2}/\d{2}/\d{2,4}'
        director_remarks = director_remarks[director_remarks['remarks'].str.contains(pattern, na=False)]

        # Extract 'from' and 'to' date strings
        director_remarks['from_date'] = director_remarks['remarks'].str.extract(r'from (\d{2}/\d{2}/\d{2})')
        director_remarks['to_date'] = director_remarks['remarks'].str.extract(r'to (\d{2}/\d{2}/\d{2})')

        # Convert date strings to datetime
        director_remarks['from_date'] = pd.to_datetime(director_remarks['from_date'], format='%d/%m/%y', errors='coerce')
        director_remarks['to_date'] = pd.to_datetime(director_remarks['to_date'], format='%d/%m/%y', errors='coerce')
        director_remarks = director_remarks[
            director_remarks['from_date'] < director_remarks['to_date']
        ]

        director_remarks = director_remarks[['din', 'from_date', 'to_date']]
        ################################################################################################################
        disq_directors = pd.merge(director_history_details, director_remarks, on=['din'])

        disq_directors = disq_directors[
            disq_directors['date_cessation'].between(disq_directors['from_date'], disq_directors['to_date'])
        ]

        if disq_directors.size == 0:
            result_df = pd.DataFrame(self.companies, columns=['company_master_id'])
            result_df['disqualified_director_active'] = 0
        else:
            companies_with_active_disq_dir = disq_directors['company_master_id'].unique()
            result_df = pd.DataFrame(self.companies, columns=['company_master_id'])
            result_df['disqualified_director_active'] = result_df.company_master_id.apply(
                lambda row: 1 if row in companies_with_active_disq_dir else 0
            )
        result_df['company_master_id'] = result_df['company_master_id'].astype('int64')
        result_df['disqualified_director_active'] = result_df['disqualified_director_active'].astype('int8')

        self.logger.info("Completed extracting disqualified directors active feature.")
        return result_df

    def disq_dir_resigned(self):
        self.logger.info("Extracting disqualified directors resigned feature.")

        director_history_details = self.director_history_details_tbl.copy()

        fy_start_date, fy_end_date = get_indian_financial_year(self.as_on_date)

        director_history_details = director_history_details[
            director_history_details['date_cessation'].between(pd.to_datetime(fy_start_date), pd.to_datetime(fy_end_date))
        ]

        director_history_details = pd.merge(director_history_details, self.director_master_tbl,
                                            on=['director_master_id'], how='inner').drop(columns=['director_master_id'])
        ################################################################################################################
        dins = director_history_details['din'].tolist()
        self.logger.info("querying director_remarks table.")
        director_remarks_qry = generate_query(
            table_name='director_remarks',
            select_columns=['din', 'remarks'],
            filter_columns={'din': 'str'},
            filter_values={'din': dins}
        )
        director_remarks = pd.read_sql(director_remarks_qry, self.live_crawler_output_connection)
        self.logger.info("completed querying director_remarks table.")

        director_remarks['remarks'] = director_remarks['remarks'].str.strip().str.lower()
        director_remarks['din'] = director_remarks['din'].str.strip().astype(int)
        director_remarks = director_remarks[
            director_remarks['din'].notnull()
        ]

        pattern = r'from \d{2}/\d{2}/\d{2,4} to \d{2}/\d{2}/\d{2,4}'
        director_remarks = director_remarks[director_remarks['remarks'].str.contains(pattern, na=False)]

        # Extract 'from' and 'to' date strings
        director_remarks['from_date'] = director_remarks['remarks'].str.extract(r'from (\d{2}/\d{2}/\d{2})')
        director_remarks['to_date'] = director_remarks['remarks'].str.extract(r'to (\d{2}/\d{2}/\d{2})')

        # Convert date strings to datetime
        director_remarks['from_date'] = pd.to_datetime(director_remarks['from_date'], format='%d/%m/%y', errors='coerce')
        director_remarks['to_date'] = pd.to_datetime(director_remarks['to_date'], format='%d/%m/%y', errors='coerce')

        director_remarks = director_remarks[
            director_remarks['from_date'] < director_remarks['to_date']
        ]

        director_remarks = director_remarks[['din', 'from_date', 'to_date']]
        ################################################################################################################
        disq_directors_resigned = pd.merge(director_history_details, director_remarks, on=['din'])

        disq_directors_resigned = disq_directors_resigned[
            disq_directors_resigned['date_cessation'].between(disq_directors_resigned['from_date'], disq_directors_resigned['to_date'])
        ]

        if disq_directors_resigned.size == 0:
            result_df = pd.DataFrame(self.companies, columns=['company_master_id'])
            result_df['disqualified_director_resigned'] = 0
        else:
            companies_with_active_disq_dir = disq_directors_resigned['company_master_id'].unique()
            result_df = pd.DataFrame(self.companies, columns=['company_master_id'])
            result_df['disqualified_director_resigned'] = result_df.company_master_id.apply(
                lambda row: 1 if row in companies_with_active_disq_dir else 0
            )

        result_df['company_master_id'] = result_df['company_master_id'].astype('int64')
        result_df['disqualified_director_resigned'] = result_df['disqualified_director_resigned'].astype('int8')
        self.logger.info("Completed extracting disqualified directors resigned feature.")
        return result_df

    def is_dir_in_d_rated_company(self):
        self.logger.info("Extracting d_rated_comp_directors features.")
        dirs_in_company_searched = self.director_history_details_tbl.copy()

        dirs_in_company_searched = dirs_in_company_searched[
            ~dirs_in_company_searched["designation"].str.contains('nominee')
        ]
        dirs_in_company_searched = dirs_in_company_searched.drop(columns=['designation'])

        dirs_in_company_searched = dirs_in_company_searched[
            dirs_in_company_searched['date_cessation'].isnull() |
            (dirs_in_company_searched['date_cessation'] > self.as_on_date)
        ]
        ################################################################################################################
        dins = dirs_in_company_searched['director_master_id'].unique().tolist()

        self.logger.info("Querying director_history_details table for d_rated_dirs.")
        director_details_qry = generate_query(
            table_name='director_history_details',
            select_columns=['company_master_id', 'director_master_id', 'designation', 'appointment_original_date',
                            'date_cessation'],
            filter_columns={'director_master_id': 'int'},
            filter_values={'director_master_id': dins}
        )
        director_in_otr_comp = pd.read_sql(director_details_qry, self.live_crawler_output_connection)
        self.logger.info("Completed querying director_history_details table for d_rated_dirs.")

        director_in_otr_comp['designation'] = director_in_otr_comp['designation'].str.strip().str.lower()

        director_in_otr_comp = director_in_otr_comp[~director_in_otr_comp["designation"].str.contains('nominee')]
        director_in_otr_comp = director_in_otr_comp.drop(columns=['designation'])

        director_in_otr_comp['appointment_original_date'] = pd.to_datetime(
            director_in_otr_comp['appointment_original_date'], format='%d/%m/%Y', errors='coerce')
        director_in_otr_comp['date_cessation'] = pd.to_datetime(director_in_otr_comp['date_cessation'],
                                                                format='%d/%m/%Y', errors='coerce')

        # Even if a director resigns, he is still liable for companies bad rating till 6 months from resigning.
        director_in_otr_comp['date_cessation'] = director_in_otr_comp['date_cessation'] + pd.DateOffset(days=182)

        director_in_otr_comp = director_in_otr_comp[
            director_in_otr_comp['date_cessation'].isnull() | (director_in_otr_comp['date_cessation'] > self.as_on_date)
        ]

        director_in_otr_comp['date_cessation'] = director_in_otr_comp.date_cessation.apply(
            lambda x: self.as_on_date if pd.isnull(x) else (self.as_on_date if x > self.as_on_date else x)
        )
        ################################################################################################################
        company_master_ids = director_in_otr_comp['company_master_id'].unique()
        self.logger.info("Querying credit_rating_data table for d_rated_dirs.")
        credit_rating_data_qry = generate_query(
            table_name='credit_rating_data',
            select_columns=['company_master_id', 'credit_rating_agency', 'derived_long_term_rating',
                            'd_date_of_last_rating'],
            filter_columns={'company_master_id': 'int'},
            filter_values={'company_master_id': company_master_ids}
        )
        credit_rating_data = pd.read_sql(credit_rating_data_qry, self.live_crawler_output_connection)
        self.logger.info("Completed querying credit_rating_data table for d_rated_dirs.")

        credit_rating_data['derived_long_term_rating'] = credit_rating_data['derived_long_term_rating'].str.lower().str.strip()
        credit_rating_data['credit_rating_agency'] = credit_rating_data['credit_rating_agency'].str.strip()
        credit_rating_data['derived_long_term_rating'] = credit_rating_data['derived_long_term_rating'].str.strip()
        credit_rating_data['d_date_of_last_rating'] = pd.to_datetime(credit_rating_data['d_date_of_last_rating'])

        credit_rating_data = credit_rating_data.rename(columns={'derived_long_term_rating': 'd_rating'}).drop_duplicates()

        credit_rating_data['rt_to_date'] = (
            credit_rating_data.sort_values(by=['d_date_of_last_rating'], ascending=False).groupby(
                ['company_master_id', 'credit_rating_agency'])['d_date_of_last_rating'].shift(1)
        )

        credit_rating_data = credit_rating_data.rename(columns={'d_date_of_last_rating': 'rt_from_date'})
        credit_rating_data = credit_rating_data.fillna(value={'rt_to_date': pd.to_datetime(pd.Timestamp.now().date())})

        credit_rating_data = credit_rating_data[
            (credit_rating_data['d_rating'] == 'd') &
            (credit_rating_data['rt_from_date'] <= credit_rating_data['rt_to_date'])
        ]

        director_in_d_rated_comp = pd.merge(director_in_otr_comp, credit_rating_data, on='company_master_id')

        director_in_d_rated_comp = director_in_d_rated_comp[
            director_in_d_rated_comp['date_cessation'].between(director_in_d_rated_comp['rt_from_date'],
                                                               director_in_d_rated_comp['rt_to_date'])
        ]

        director_in_d_rated_comp = director_in_d_rated_comp[['company_master_id', 'director_master_id']]
        director_in_d_rated_comp = director_in_d_rated_comp.rename(
            columns={'company_master_id': 'def_company_master_id'}
        ).drop_duplicates()

        ################################################################################################################
        d_rated_comp_dir_on_board = pd.merge(dirs_in_company_searched, director_in_d_rated_comp,
                                             on='director_master_id')
        d_rated_comp_dir_on_board = d_rated_comp_dir_on_board[
            (d_rated_comp_dir_on_board['def_company_master_id'] != d_rated_comp_dir_on_board['company_master_id'])
        ]

        d_rated_comp_dir_on_board_otr_comp = d_rated_comp_dir_on_board[['company_master_id']].drop_duplicates()

        if d_rated_comp_dir_on_board_otr_comp.size == 0:
            result_df = pd.DataFrame(self.companies, columns=['company_master_id'])
            result_df['director_in_d_rated_comp'] = 0
        else:
            companies_with_d_rated_dir = d_rated_comp_dir_on_board_otr_comp['company_master_id'].unique()
            result_df = pd.DataFrame(self.companies, columns=['company_master_id'])
            result_df['director_in_d_rated_comp'] = result_df.company_master_id.apply(
                lambda row: 1 if row in companies_with_d_rated_dir else 0
            )

        result_df['company_master_id'] = result_df['company_master_id'].astype('int64')
        result_df['director_in_d_rated_comp'] = result_df['director_in_d_rated_comp'].astype('int8')
        result_df = result_df.drop_duplicates()
        self.logger.info("Completed extracting d_rated_comp_directors features.")
        return result_df

    def is_dir_in_cibil_defaulted_company(self):
        self.logger.info("Extracting cibil_defaulted_comp_directors features.")
        dirs_in_company_searched = self.director_history_details_tbl.copy()

        dirs_in_company_searched = dirs_in_company_searched[
            ~dirs_in_company_searched["designation"].str.contains('nominee')
        ]
        dirs_in_company_searched = dirs_in_company_searched.drop(columns=['designation'])

        dirs_in_company_searched = dirs_in_company_searched[
            dirs_in_company_searched['date_cessation'].isnull() |
            (dirs_in_company_searched['date_cessation'] > self.as_on_date)
        ]
        ################################################################################################################
        dins = dirs_in_company_searched['director_master_id'].unique().tolist()

        self.logger.info("Querying director_history_details table for cibil_defaulted_dirs.")
        director_details_qry = generate_query(
            table_name='director_history_details',
            select_columns=['company_master_id', 'director_master_id', 'designation', 'appointment_original_date',
                            'date_cessation'],
            filter_columns={'director_master_id': 'int'},
            filter_values={'director_master_id': dins}
        )
        director_in_otr_comp = pd.read_sql(director_details_qry, self.live_crawler_output_connection)
        self.logger.info("Completed querying director_history_details table for cibil_defaulted_dirs.")
        director_in_otr_comp['designation'] = director_in_otr_comp['designation'].str.strip().str.lower()

        director_in_otr_comp = director_in_otr_comp[~director_in_otr_comp["designation"].str.contains('nominee')]
        director_in_otr_comp = director_in_otr_comp.drop(columns=['designation'])

        director_in_otr_comp['appointment_original_date'] = pd.to_datetime(
            director_in_otr_comp['appointment_original_date'], format='%d/%m/%Y', errors='coerce')
        director_in_otr_comp['date_cessation'] = pd.to_datetime(director_in_otr_comp['date_cessation'],
                                                                format='%d/%m/%Y', errors='coerce')

        # Even if a director resigns, he is still liable for companies bad rating till 6 months from resigning.
        director_in_otr_comp['date_cessation'] = director_in_otr_comp['date_cessation'] + pd.DateOffset(days=182)

        director_in_otr_comp = director_in_otr_comp[
            director_in_otr_comp['date_cessation'].isnull() | (director_in_otr_comp['date_cessation'] > self.as_on_date)
        ]

        director_in_otr_comp['date_cessation'] = director_in_otr_comp.date_cessation.apply(
            lambda x: self.as_on_date if pd.isnull(x) else (self.as_on_date if x > self.as_on_date else x)
        )
        ################################################################################################################
        company_master_ids = director_in_otr_comp['company_master_id'].unique()
        self.logger.info("Completed querying cibil_defaulter_data table for cibil_defaulted_dirs.")
        cibil_defaulter_data_qry = generate_query(
            table_name='cibil_defaulter_data',
            select_columns=['company_master_id', 'credit_bureau', 'd_quarter'],
            filter_columns={'company_master_id': 'int'},
            filter_values={'company_master_id': company_master_ids}
        )
        cibil_defaulter_data = pd.read_sql(cibil_defaulter_data_qry, self.live_crawler_output_connection)
        self.logger.info("Completed querying cibil_defaulter_data table for cibil_defaulted_dirs.")

        cibil_defaulter_data['credit_bureau'] = cibil_defaulter_data['credit_bureau'].str.strip().str.lower()
        cibil_defaulter_data['d_quarter'] = pd.to_datetime(cibil_defaulter_data['d_quarter'])
        cibil_defaulter_data = cibil_defaulter_data[
            (cibil_defaulter_data['credit_bureau'].notnull()) &
            (cibil_defaulter_data['d_quarter'].notnull())
        ]

        cibil_defaulter_data['default_from_date'] = cibil_defaulter_data.apply(
            lambda row: pd.to_datetime(row['d_quarter'].to_period('Q').start_time.date())
            if row['credit_bureau'] == 'cibil' else pd.to_datetime(row['d_quarter'].to_period('M').start_time.date()),
            axis=1
        )
        cibil_defaulter_data['default_to_date'] = cibil_defaulter_data.apply(
            lambda row: pd.to_datetime(row['d_quarter'].to_period('Q').end_time.date())
            if row['credit_bureau'] == 'cibil' else pd.to_datetime(row['d_quarter'].to_period('M').end_time.date()),
            axis=1
        )
        cibil_defaulter_data = cibil_defaulter_data[
            (cibil_defaulter_data['default_from_date'] <= self.as_on_date) &
            (self.as_on_date <= cibil_defaulter_data['default_to_date'])
        ].drop_duplicates()
        cibil_defaulter_data = cibil_defaulter_data.drop(columns=['d_quarter'])
        ################################################################################################################
        director_in_cibil_defaulted_comp = pd.merge(director_in_otr_comp, cibil_defaulter_data, on='company_master_id')

        director_in_cibil_defaulted_comp = director_in_cibil_defaulted_comp[
            director_in_cibil_defaulted_comp['date_cessation'].between(director_in_cibil_defaulted_comp['default_from_date'],
                                                                       director_in_cibil_defaulted_comp['default_to_date'])
        ]

        director_in_cibil_defaulted_comp = director_in_cibil_defaulted_comp[['company_master_id', 'director_master_id']].drop_duplicates()
        director_in_cibil_defaulted_comp = director_in_cibil_defaulted_comp.rename(
            columns={'company_master_id': 'def_company_master_id'}
        )
        ################################################################################################################
        cibil_defaulted_comp_dir_on_board = pd.merge(dirs_in_company_searched, director_in_cibil_defaulted_comp,
                                                     on='director_master_id')

        cibil_defaulted_comp_dir_on_board = cibil_defaulted_comp_dir_on_board[
            (cibil_defaulted_comp_dir_on_board['def_company_master_id'] != cibil_defaulted_comp_dir_on_board['company_master_id'])
        ]

        cibil_defaulted_comp_dir_on_board = cibil_defaulted_comp_dir_on_board[['company_master_id']].drop_duplicates()

        if cibil_defaulted_comp_dir_on_board.size == 0:
            result_df = pd.DataFrame(self.companies, columns=['company_master_id'])
            result_df['director_in_defaulted_comp'] = 0
        else:
            companies_with_d_rated_dir = cibil_defaulted_comp_dir_on_board['company_master_id'].unique()
            result_df = pd.DataFrame(self.companies, columns=['company_master_id'])
            result_df['director_in_defaulted_comp'] = result_df.company_master_id.apply(
                lambda row: 1 if row in companies_with_d_rated_dir else 0
            )

        result_df['company_master_id'] = result_df['company_master_id'].astype('int64')
        result_df['director_in_defaulted_comp'] = result_df['director_in_defaulted_comp'].astype('int8')
        self.logger.info("Completed Extracting cibil_defaulted_comp_directors features.")
        return result_df
