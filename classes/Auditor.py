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
from datetime import datetime
from itertools import product

import pandas as pd
from classes.Utils import ConnectionUtils, generate_query, get_logger

companies_list = [i for i in range(760, 770)]


class AuditorFeatures:
    """
    Class to generate Auditor Features
    """

    def __init__(self):
        self.logger = get_logger('AuditorLogger', logging.INFO)
        self.as_on_dt_fy = None

    @staticmethod
    def prepare_dummy_records(company_ids: list, min_yr, max_yr):
        years_list = [f"{yr}-{yr + 1}" for yr in range(min_yr, max_yr)]

        combinations = list(product(company_ids, years_list))

        result_df = pd.DataFrame(combinations, columns=['company_master_id', 'financial_year'])
        result_df['company_master_id'] = result_df['company_master_id'].astype('int')
        result_df['financial_year'] = result_df['financial_year'].astype('str')
        return result_df

    @staticmethod
    def calculate_financial_year(row: pd.Series) -> str:
        if row['aud_resign_month'] <= 3:
            return f"{row['aud_resign_year'] - 1}-{row['aud_resign_year']}"
        else:
            return f"{row['aud_resign_year']}-{row['aud_resign_year'] + 1}"

    def build_agg_features(self, data: pd.DataFrame, feature_prefix, feature_col, agg_type='sum'):
        data[feature_col].fillna(0.5, inplace=True)

        if agg_type == 'sum':
            agg_func = 'sum'
        elif agg_type == 'mean':
            agg_func = 'mean'
        else:
            raise ValueError('Invalid aggregate type')

        data = data.sort_values(by=['company_master_id', 'financial_year_start'], ascending=True, ignore_index=True)

        result = data.assign(
            **{
                f'{feature_prefix}_1yrs': data.groupby('company_master_id')[feature_col].apply(
                    lambda x: x.rolling(window=1).agg(agg_func)
                ),
                f'{feature_prefix}_2yrs': data.groupby('company_master_id')[feature_col].apply(
                    lambda x: x.rolling(window=2).agg(agg_func)
                ),
                f'{feature_prefix}_3yrs': data.groupby('company_master_id')[feature_col].apply(
                    lambda x: x.rolling(window=3).agg(agg_func)
                ),
                f'{feature_prefix}_4yrs': data.groupby('company_master_id')[feature_col].apply(
                    lambda x: x.rolling(window=4).agg(agg_func)
                )
            }
        )

        self.logger.info(f"Aggregates calculated for col: {feature_col}.")

        result = result[result['financial_year'] == self.as_on_dt_fy]
        result = result[['company_master_id', 'financial_year', f'{feature_prefix}_1yrs', f'{feature_prefix}_2yrs',
                         f'{feature_prefix}_3yrs', f'{feature_prefix}_4yrs']].drop_duplicates()
        return result

    def get_dataset(self):
        self.logger.info("Auditors Feature Extraction Started.")
        connection = ConnectionUtils()
        etl_etl_data_connection = connection.get_db_connection(connection_name='etl_etl_data')
        ################################################################################################################
        as_on_dt = datetime(2020, 5, 17).date()
        end_year = as_on_dt.year - 1 if as_on_dt.month <= 3 else as_on_dt.year
        start_year = end_year - 4
        valid_years = [f'{i}-{i + 1}' for i in range(start_year, end_year)]
        as_on_dt_fy = f"{as_on_dt.year - 2}-{as_on_dt.year - 1}" if as_on_dt.month <= 3 else f"{as_on_dt.year - 1}-{as_on_dt.year}"
        self.as_on_dt_fy = as_on_dt_fy
        ################################################################################################################
        form_aoc4_dtls_query = generate_query(
            table_name='form_aoc4_dtls',
            select_columns=['company_master_id', 'financial_year', 'valid_data', 'inserted_at',
                            'is_consolidated_standalone_stmt', 'wthr_fin_statement_qualify'],
            filter_columns={'company_master_id': 'int', 'financial_year': 'str'},
            filter_values={'company_master_id': companies_list, 'financial_year': valid_years}
        )

        form_aoc4_dtls = pd.read_sql(form_aoc4_dtls_query, etl_etl_data_connection)

        form_aoc4_dtls['financial_year'] = form_aoc4_dtls['financial_year'].str.strip()
        form_aoc4_dtls['is_consolidated_standalone_stmt'] = form_aoc4_dtls[
            'is_consolidated_standalone_stmt'].str.strip().str.lower()
        form_aoc4_dtls['wthr_fin_statement_qualify'] = form_aoc4_dtls[
            'wthr_fin_statement_qualify'].str.strip().str.lower()
        form_aoc4_dtls = form_aoc4_dtls.rename(columns={'wthr_fin_statement_qualify': 'wthr_qualified'})

        form_aoc4_dtls = form_aoc4_dtls[
            (form_aoc4_dtls['financial_year'].isin(valid_years)) &
            (form_aoc4_dtls['valid_data'] == 1)
            ]
        form_aoc4_dtls = form_aoc4_dtls.drop(['valid_data'], axis=1)

        window = form_aoc4_dtls.groupby(['company_master_id', 'financial_year', 'is_consolidated_standalone_stmt'])[
            'inserted_at'].rank(method='first', ascending=False)

        form_aoc4_dtls['row'] = window

        form_aoc4_dtls = form_aoc4_dtls[form_aoc4_dtls['row'] == 1].drop(['inserted_at', 'row'], axis=1)
        self.logger.info(f"form_aoc4_dtls transformations done. Number of Rows: {len(form_aoc4_dtls.index)}")
        ################################################################################################################
        aoc4_auditor_dtls_query = generate_query(
            table_name='aoc4_auditor_dtls',
            select_columns=['company_master_id', 'financial_year', 'is_consolidated_standalone_stmt', 'pan_no',
                            't_category'],
            filter_columns={'company_master_id': 'int', 'financial_year': 'str'},
            filter_values={'company_master_id': companies_list, 'financial_year': valid_years}
        )

        aoc4_auditor_dtls = pd.read_sql(aoc4_auditor_dtls_query, etl_etl_data_connection)

        aoc4_auditor_dtls['financial_year'] = aoc4_auditor_dtls['financial_year'].str.strip()
        aoc4_auditor_dtls['is_consolidated_standalone_stmt'] = aoc4_auditor_dtls[
            'is_consolidated_standalone_stmt'].str.strip().str.lower()
        aoc4_auditor_dtls['t_category'] = aoc4_auditor_dtls['t_category'].str.strip().str.lower()
        aoc4_auditor_dtls['pan_no'] = aoc4_auditor_dtls['pan_no'].str.strip().str.upper()

        aoc4_auditor_dtls = aoc4_auditor_dtls[
            (aoc4_auditor_dtls['financial_year'].isin(valid_years))
        ].drop_duplicates()

        aoc4_auditor_dtls = pd.merge(aoc4_auditor_dtls, form_aoc4_dtls,
                                     on=['company_master_id', 'financial_year', 'is_consolidated_standalone_stmt'],
                                     how='left')

        aoc4_auditor_dtls = aoc4_auditor_dtls[['company_master_id', 'financial_year', 'pan_no', 't_category',
                                               'wthr_qualified', 'is_consolidated_standalone_stmt']].drop_duplicates()
        self.logger.info(f"aoc4_auditor_dtls transformations done. Number of Rows: {len(aoc4_auditor_dtls.index)}")
        ################################################################################################################
        aoc4xbrl_auditor_dtls_query = generate_query(
            table_name='aoc4xbrl_auditor_dtls',
            select_columns=['company_master_id', 'aud_firm_pan', 'category', 'is_consolidated_standalone_stmt',
                            'wthr_qualified', 'financial_year'],
            filter_columns={'company_master_id': 'int', 'financial_year': 'str'},
            filter_values={'company_master_id': companies_list, 'financial_year': valid_years}
        )

        aoc4xbrl_auditor_dtls = pd.read_sql(aoc4xbrl_auditor_dtls_query, etl_etl_data_connection)
        aoc4xbrl_auditor_dtls['aud_firm_pan'] = aoc4xbrl_auditor_dtls['aud_firm_pan'].str.upper().str.strip()
        aoc4xbrl_auditor_dtls = aoc4xbrl_auditor_dtls.rename(columns={'aud_firm_pan': 'pan_no'})
        aoc4xbrl_auditor_dtls['category'] = aoc4xbrl_auditor_dtls['category'].str.lower().str.strip()
        aoc4xbrl_auditor_dtls = aoc4xbrl_auditor_dtls.rename(columns={'category': 't_category'})
        aoc4xbrl_auditor_dtls['is_consolidated_standalone_stmt'] = aoc4xbrl_auditor_dtls[
            'is_consolidated_standalone_stmt'].str.lower().str.strip()
        aoc4xbrl_auditor_dtls['wthr_qualified'] = aoc4xbrl_auditor_dtls['wthr_qualified'].str.lower().str.strip()
        aoc4xbrl_auditor_dtls['financial_year'] = aoc4xbrl_auditor_dtls['financial_year'].str.strip()

        aoc4xbrl_auditor_dtls = aoc4xbrl_auditor_dtls[
            aoc4xbrl_auditor_dtls['financial_year'].isin(valid_years)
        ].drop_duplicates()
        self.logger.info(f"aoc4xbrl_auditor_dtls transformations done. Rows: {len(aoc4xbrl_auditor_dtls.index)}")
        ################################################################################################################
        adt3_general_resignation_dtls_query = generate_query(
            table_name='adt3_general_resignation_dtls',
            select_columns=['company_master_id', 'auditor_pan', 'auditor_firm', 'auditor_resignation_date'],
            filter_columns={'company_master_id': 'int', 'financial_year': 'str'},
            filter_values={'company_master_id': companies_list, 'financial_year': valid_years}
        )

        auditor_resignation = pd.read_sql(adt3_general_resignation_dtls_query, etl_etl_data_connection)

        auditor_resignation['auditor_pan'] = auditor_resignation['auditor_pan'].str.upper().str.strip()
        auditor_resignation['auditor_firm'] = auditor_resignation['auditor_firm'].str.strip()
        auditor_resignation['auditor_resignation_date'] = pd.to_datetime(
            auditor_resignation['auditor_resignation_date'],
            format='%Y-%m-%d'
        )
        auditor_resignation = auditor_resignation[
            (auditor_resignation['auditor_resignation_date'].notnull())
        ].drop_duplicates()

        auditor_resignation['aud_resign_month'] = auditor_resignation['auditor_resignation_date'].dt.month
        auditor_resignation['aud_resign_year'] = auditor_resignation['auditor_resignation_date'].dt.year

        auditor_resignation['financial_year'] = auditor_resignation.apply(self.calculate_financial_year, axis=1)
        auditor_resignation = auditor_resignation.drop(['aud_resign_month', 'aud_resign_year'], axis=1)
        auditor_resignation = auditor_resignation[
            auditor_resignation['financial_year'].isin(valid_years)
        ]
        auditor_resignation['has_auditor_resigned'] = 1

        self.logger.info(f"auditor_resignation transformations done. Number of Rows: {len(auditor_resignation.index)}")
        ################################################################################################################
        auditor_dtls = pd.concat([aoc4_auditor_dtls, aoc4xbrl_auditor_dtls], ignore_index=True)

        auditor_dtls['not_qualified'] = auditor_dtls['wthr_qualified'].eq('no')
        auditor_dtls['auditor_pan'] = auditor_dtls['pan_no'].str.replace('"', '').str.split(',').explode()
        auditor_dtls = auditor_dtls.dropna(subset=['auditor_pan']).drop(['pan_no'], axis=1)

        auditor_dtls = pd.merge(auditor_dtls, auditor_resignation,
                                on=['financial_year', 'auditor_pan', 'company_master_id'], how='left')

        auditor_dtls = auditor_dtls.fillna(value={'has_auditor_resigned': 0})
        auditor_dtls['adv_qualified'] = auditor_dtls['not_qualified'].astype('int32')
        auditor_dtls['adverse_qualf_and_resigned'] = auditor_dtls['has_auditor_resigned'] * auditor_dtls[
            'adv_qualified']

        window = auditor_dtls.groupby(
            ['company_master_id', 'auditor_pan', 'is_consolidated_standalone_stmt', 'financial_year']
        )['financial_year'].rank(method="first", ascending=False)

        auditor_dtls['row'] = window

        auditor_dtls = auditor_dtls[auditor_dtls['row'] == 1]

        auditor_dtls = auditor_dtls[
            ['company_master_id', 'financial_year', 't_category', 'auditor_pan', 'auditor_firm',
             'is_consolidated_standalone_stmt', 'wthr_qualified', 'not_qualified', 'auditor_resignation_date',
             'has_auditor_resigned', 'adv_qualified', 'adverse_qualf_and_resigned']
        ]

        self.logger.info(f"auditor_dtls created. Number of Rows: {len(auditor_dtls.index)}")

        ################################################################################################################
        auditor_dummy_records = self.prepare_dummy_records(companies_list, start_year, end_year)
        auditor_dtls = pd.merge(auditor_dtls, auditor_dummy_records, on=['company_master_id', 'financial_year'],
                                how='outer')
        auditor_dtls['financial_year_start'] = auditor_dtls['financial_year'].str.split('-').str[0].astype(int)

        auditor_dtls = auditor_dtls[auditor_dtls['financial_year_start'].notnull()]
        self.logger.info(f"dummy_records added in required to auditor_dtls. Number of Rows: {len(auditor_dtls.index)}")
        ################################################################################################################
        adverse_qual_score = self.build_agg_features(auditor_dtls, 'adv_qual_score', 'adv_qualified', 'mean')

        adverse_resigned_score = self.build_agg_features(auditor_dtls, 'auditor_resigned_score', 'has_auditor_resigned',
                                                         'mean')

        adverse_qual_and_resigned_score = self.build_agg_features(auditor_dtls, 'adv_qual_and_resigned_score',
                                                                  'adverse_qualf_and_resigned', 'mean')

        audit_score = pd.merge(adverse_qual_score, adverse_resigned_score, on=['company_master_id', 'financial_year'],
                               how='outer')

        audit_score = pd.merge(adverse_qual_and_resigned_score, audit_score, on=['company_master_id', 'financial_year'],
                               how='outer')

        ################################################################################################################
        aoc4_caro_dtls_query = generate_query(
            table_name='aoc4_caro_dtls',
            select_columns=['company_master_id', 'financial_year', 't_fixed_assets', 't_fraud_noticed',
                            'is_consolidated_standalone_stmt', 't_statutory_dues', 't_accept_public_deposits',
                            't_term_loans', 't_inventories'],
            filter_columns={'company_master_id': 'int', 'financial_year': 'str'},
            filter_values={'company_master_id': companies_list, 'financial_year': valid_years}
        )
        aoc4_caro_dtls = pd.read_sql(aoc4_caro_dtls_query, etl_etl_data_connection)

        aoc4_caro_dtls['is_consolidated_standalone_stmt'] = aoc4_caro_dtls[
            'is_consolidated_standalone_stmt'].str.lower().str.strip()
        aoc4_caro_dtls['financial_year'] = aoc4_caro_dtls['financial_year'].str.strip()

        caro_standalone = aoc4_caro_dtls[
            (aoc4_caro_dtls['is_consolidated_standalone_stmt'] == 'standalone') &
            (aoc4_caro_dtls['financial_year'].isin(valid_years))
            ]

        caro_standalone = caro_standalone[
            ['company_master_id', 'financial_year', 't_fixed_assets', 't_fraud_noticed', 't_statutory_dues',
             't_accept_public_deposits', 't_term_loans', 't_inventories']
        ]
        ################################################################################################################
        # ----------------------------- Remove this Line After uncommenting Above Code ------------------------------- #
        ################################################################################################################
        auditor_dummy_records = self.prepare_dummy_records(companies_list, start_year, end_year)
        ################################################################################################################
        ################################################################################################################
        caro_standalone = pd.merge(caro_standalone, auditor_dummy_records, on=['company_master_id', 'financial_year'],
                                   how='outer')

        ################################################################################################################
        caro_fixed_assets = caro_standalone.groupby(['company_master_id', 'financial_year']) \
            .agg({'t_fixed_assets': lambda x: set(x.str.lower())}) \
            .reset_index() \
            .assign(fixed_assets_score=lambda df: df['t_fixed_assets'].apply(
             lambda s: 0 if 'unfavourable' in s or 'disclaimer' in s else (1 if 'favourable' in s else 0.5)
            )
        )

        caro_fixed_assets['financial_year_start'] = caro_fixed_assets['financial_year'].str.split('-').str[0].astype(int)
        caro_fixed_assets = caro_fixed_assets[caro_fixed_assets['financial_year_start'].notnull()]
        caro_fixed_assets = caro_fixed_assets[
            ['company_master_id', 'financial_year', 'financial_year_start', 'fixed_assets_score']
        ]

        f_asset_score = self.build_agg_features(caro_fixed_assets, 'fixed_assets_score', 'fixed_assets_score', 'mean')

        self.logger.info(f"extraction of 'f_asset_score' features completed.")
        ################################################################################################################
        caro_default = caro_standalone.groupby(['company_master_id', 'financial_year']) \
            .agg({'t_fraud_noticed': lambda x: set(x.str.lower())}) \
            .reset_index() \
            .assign(default_score=lambda df: df['t_fraud_noticed'].apply(
             lambda s: 0 if 'unfavourable' in s or 'disclaimer' in s else (1 if 'favourable' in s else 0.5)
            )
        )

        caro_default['financial_year_start'] = caro_default['financial_year'].str.split('-').str[0].astype(int)
        caro_default = caro_default[caro_default['financial_year_start'].notnull()]
        caro_default = caro_default[
            ['company_master_id', 'financial_year', 'financial_year_start', 'default_score']
        ]

        fraud_score = self.build_agg_features(caro_default, 'fraud_score', 'default_score', 'mean')

        self.logger.info(f"extraction of 'default_score' features completed.")
        ################################################################################################################
        statutory_dues = caro_standalone.groupby(['company_master_id', 'financial_year']) \
            .agg({'t_statutory_dues': lambda x: set(x.str.lower())}) \
            .reset_index() \
            .assign(due_score=lambda df: df['t_statutory_dues'].apply(
             lambda s: 0 if 'unfavourable' in s or 'disclaimer' in s else (1 if 'favourable' in s else 0.5)
            )
        )

        statutory_dues['financial_year_start'] = statutory_dues['financial_year'].str.split('-').str[0].astype(int)
        statutory_dues = statutory_dues[statutory_dues['financial_year_start'].notnull()]
        statutory_dues = statutory_dues[
            ['company_master_id', 'financial_year', 'financial_year_start', 'due_score']
        ]

        statutory_due_score = self.build_agg_features(statutory_dues, 'statutory_due_score', 'due_score', 'mean')

        self.logger.info(f"extraction of 'due_score' features completed.")
        ################################################################################################################
        financial_dues = caro_standalone.groupby(['company_master_id', 'financial_year']) \
            .agg({'t_accept_public_deposits': lambda x: set(x.str.lower())}) \
            .reset_index() \
            .assign(financial_due_score=lambda df: df['t_accept_public_deposits'].apply(
             lambda s: 0 if 'unfavourable' in s or 'disclaimer' in s else (1 if 'favourable' in s else 0.5)
            )
        )

        financial_dues['financial_year_start'] = financial_dues['financial_year'].str.split('-').str[0].astype(int)
        financial_dues = financial_dues[financial_dues['financial_year_start'].notnull()]
        financial_dues = financial_dues[
            ['company_master_id', 'financial_year', 'financial_year_start', 'financial_due_score']
        ]

        financial_due_score = self.build_agg_features(financial_dues, 'financial_due_score', 'financial_due_score', 'mean')

        self.logger.info(f"extraction of 'financial_due_score' features completed.")
        ################################################################################################################
        term_loan_dues = caro_standalone.groupby(['company_master_id', 'financial_year']) \
            .agg({'t_term_loans': lambda x: set(x.str.lower())}) \
            .reset_index() \
            .assign(term_loans_score=lambda df: df['t_term_loans'].apply(
             lambda s: 0 if 'unfavourable' in s or 'disclaimer' in s else (1 if 'favourable' in s else 0.5)
            )
        )

        term_loan_dues['financial_year_start'] = term_loan_dues['financial_year'].str.split('-').str[0].astype(int)
        term_loan_dues = term_loan_dues[term_loan_dues['financial_year_start'].notnull()]
        term_loan_dues = term_loan_dues[
            ['company_master_id', 'financial_year', 'financial_year_start', 'term_loans_score']
        ]

        term_loan_score = self.build_agg_features(term_loan_dues, 'term_loans_score', 'term_loans_score', 'mean')

        self.logger.info(f"extraction of 'term_loans_score' features completed.")
        ################################################################################################################
        caro_inventories = caro_standalone.groupby(['company_master_id', 'financial_year']) \
            .agg({'t_inventories': lambda x: set(x.str.lower())}) \
            .reset_index() \
            .assign(inventories_score=lambda df: df['t_inventories'].apply(
             lambda s: 0 if 'unfavourable' in s or 'disclaimer' in s else (1 if 'favourable' in s else 0.5)
            )
        )

        caro_inventories['financial_year_start'] = caro_inventories['financial_year'].str.split('-').str[0].astype(int)
        caro_inventories = caro_inventories[term_loan_dues['financial_year_start'].notnull()]
        caro_inventories = caro_inventories[
            ['company_master_id', 'financial_year', 'financial_year_start', 'inventories_score']
        ]

        inventory_score = self.build_agg_features(caro_inventories, 'inventories_score', 'inventories_score', 'mean')

        self.logger.info(f"extraction of 'inventories_score' features completed.")
        ################################################################################################################

        final_score = pd.merge(f_asset_score, fraud_score, on=['financial_year', 'company_master_id'], how='outer')
        final_score = pd.merge(statutory_due_score, final_score, on=['financial_year', 'company_master_id'], how='outer')
        final_score = pd.merge(financial_due_score, final_score, on=['financial_year', 'company_master_id'], how='outer')
        final_score = pd.merge(term_loan_score, final_score, on=['financial_year', 'company_master_id'], how='outer')
        final_score = pd.merge(inventory_score, final_score, on=['financial_year', 'company_master_id'], how='outer')

        final_score = pd.merge(final_score, audit_score, on=['company_master_id', 'financial_year'], 
                               how='outer').drop_duplicates()
        print(final_score.to_string())
        self.logger.info("Auditors Feature Extraction Completed.")


AuditorFeatures().get_dataset()
