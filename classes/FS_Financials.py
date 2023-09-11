#  Author : Naga Satish Chitturi(CA0316)
#  Created : 2023-08-20
#  Last Modified : 2020-02-23
#  Description : This class contains related to FinanceServicesCompanies Financial features(KPIs).
#
#  Change Log
#  ---------------------------------------------------------------------------
#   Date			Author			Comment
#  ---------------------------------------------------------------------------
#

import logging
from datetime import datetime

import numpy as np
import pandas as pd
from classes import Utils, get_logger, generate_query

utils = Utils()

pnl_data_drop_cols = ('preferred_stmt', 'prev_rep_to', 'cin', 'form_master_id', 'last_modified', 'form_id',
                      'valid_data', 'is_manually_updated_prev', 'datasource', 'document_date', 'is_latest_data',
                      'file_id', 'document_id', 'version_number', 'curr_rep_from', 'prev_rep_from',
                      'is_manually_updated', 'doc_info_document_id', 'original_file_id', 'curr_rep_to')

fin_ratio_data_drop_cols = ('is_manually_updated_prev', 'preferred_stmt', 'document_date', 'is_latest_data',
                            'cin', 'form_id', 'is_manually_updated', 'file_id', 'form_master_id', 'document_id',
                            'doc_info_document_id', 'valid_data', 'version_number', 'datasource')


class FsFinancialFeatures:
    def __init__(self, companies_list: list, as_on_date: datetime.date):
        """
        Class to generate Financial Features of Financial Services Companies
        :param companies_list: list of companies to extract features
        :param as_on_date: A date on which features should be extracted for the given set of companies.
        """
        self.logger = get_logger('FsFinancialsLogger', logging.INFO)
        self.companies = companies_list
        self.as_on_date = as_on_date
        self.end_year = self.as_on_date.year - 1 if self.as_on_date.month <= 3 else self.as_on_date.year
        self.start_year = self.end_year - 4
        as_on_dt_fy = f"{self.as_on_date.year - 2}-{self.as_on_date.year - 1}" if self.as_on_date.month <= 3 else f"{self.as_on_date.year - 1}-{self.as_on_date.year}"

        self.as_on_dt_fy = as_on_dt_fy
        self.valid_years = [f'{i}-{i + 1}' for i in range(self.start_year, self.end_year)]

    def extract_financial(self, bs_data: pd.DataFrame, pnl_data: pd.DataFrame, fin_ratio_data: pd.DataFrame):
        """
        Function to extract Financial features for Financial Services Companies.
        :param bs_data: dataframe with Accord MCA Balance Sheet data
        :param pnl_data: dataframe with Accord MCA Profit Loss data
        :param fin_ratio_data: dataframe with Financial Ratios data(not Accord MCA Financial Ratios)
        :return: DataFrame
        """
        self.logger.info('started extracting financials for FS Companies')

        bs_data['is_consolidated_standalone_stmt'] = bs_data['is_consolidated_standalone_stmt'].str.lower().str.strip()
        pnl_data['is_consolidated_standalone_stmt'] = pnl_data[
            'is_consolidated_standalone_stmt'].str.lower().str.strip()
        fin_ratio_data['is_consolidated_standalone_stmt'] = fin_ratio_data[
            'is_consolidated_standalone_stmt'].str.lower().str.strip()
        ################################################################################################################
        win_bs = bs_data.sort_values(by=['accord_mca_balance_sheet_dtls_id']).groupby(
            by=['company_master_id', 'financial_year', 'is_consolidated_standalone_stmt'], group_keys=True
        )['accord_mca_balance_sheet_dtls_id'].rank(method='first', ascending=False)

        bs_data['id1'] = win_bs
        bs_data = bs_data[(bs_data['id1'] == 1)].drop(columns=['id1'])
        ################################################################################################################
        win_pnl = pnl_data.sort_values(by=['accord_mca_profit_loss_id']).groupby(
            by=['company_master_id', 'financial_year', 'is_consolidated_standalone_stmt'], group_keys=True
        )['accord_mca_profit_loss_id'].rank(method='first', ascending=False)

        pnl_data['id1'] = win_pnl
        pnl_data = pnl_data[(pnl_data['id1'] == 1)].drop(columns=['id1'])
        ################################################################################################################
        win_fin_ratio = fin_ratio_data.sort_values(by=['financial_ratios_id']).groupby(
            by=['company_master_id', 'financial_year', 'is_consolidated_standalone_stmt'], group_keys=True
        )['financial_ratios_id'].rank(method='first', ascending=False)

        fin_ratio_data['id1'] = win_fin_ratio
        fin_ratio_data = fin_ratio_data[(fin_ratio_data['id1'] == 1)].drop(columns=['id1'])
        ################################################################################################################
        fs_data = pd.merge(bs_data, pnl_data,
                           on=['company_master_id', 'financial_year', 'is_consolidated_standalone_stmt'])
        fs_data = pd.merge(fs_data, fin_ratio_data,
                           on=['company_master_id', 'financial_year', 'is_consolidated_standalone_stmt'], how='left')
        ################################################################################################################
        fs_data['preference_order'] = fs_data.apply(
            lambda row: 0 if row['is_consolidated_standalone_stmt'] == 'standalone' else 1, axis=1
        )

        pref_win = fs_data.sort_values(by='preference_order', ascending=False).groupby(
            ['company_master_id', 'financial_year'], group_keys=True)['company_master_id'].rank(
            method='first', ascending=False)
        fs_data['id2'] = pref_win
        fs_data = fs_data[(fs_data['id2'] == 1)].drop(columns=['id2'])
        ################################################################################################################
        fs_data['total_assets_cur'] = fs_data.apply(
            lambda row: row['t_tot_assets_curr'] if row['form_id'] in [3, 39] else row['tot_assets_curr'], axis=1
        )

        fs_data['total_assets_prev'] = fs_data.apply(
            lambda row: row['t_tot_assets_prev'] if row['form_id'] in [3, 39] else row['tot_assets_prev'], axis=1
        )

        fs_data['networth_curr'] = fs_data.apply(
            lambda row: row['t_networth_curr'] if row['form_id'] in [37, 38] else row['t_tot_share_hold_fund_curr'],
            axis=1
        )

        fs_data['networth_prev'] = fs_data.apply(
            lambda row: row['t_networth_prev'] if row['form_id'] in [37, 38] else row['t_tot_share_hold_fund_prev'],
            axis=1
        )

        fs_data['roa'] = fs_data.apply(lambda row: 2 * (
                row['t_tot_profit_loss_for_period_curr'] / sum(filter(None, [row['total_assets_cur'],
                                                                             row['total_assets_prev']]))
        ) if sum(filter(None, [row['total_assets_cur'], row['total_assets_prev']])) != 0 else None, axis=1)

        fs_data['roe_curr'] = fs_data.apply(
            lambda row: row['t_tot_profit_loss_for_period_curr'] / row['networth_curr'] if row['networth_curr'] not in [0, None]
            else None, axis=1
        )
        fs_data['roe_prev'] = fs_data.apply(
            lambda row: row['t_tot_profit_loss_for_period_prev'] / row['networth_prev'] if row['networth_prev'] not in [0, None]
            else None, axis=1
        )

        fs_data['roe'] = fs_data['roe_curr']

        to_float_cols = ['intreset_inc_curr', 'loan_curr', 'fince_cost_curr', 'debt_security_curr', 'borrowings_curr',
                         'dep_curr', 'subordinated_liab_curr', 'intreset_inc_prev', 'loan_prev', 'fince_cost_prev',
                         'debt_security_prev', 'borrowings_prev', 'dep_prev', 'subordinated_liab_prev',
                         'lng_term_borrow_curr', 'shrt_term_borrow_curr', 'lng_term_borrow_prev',
                         'shrt_term_borrow_prev']

        for col in to_float_cols:
            fs_data[col] = fs_data[col].astype('float')

        fs_data['spread'] = fs_data.apply(
            lambda row: sum(filter(None, [(row['intreset_inc_curr'] / row['loan_curr']), -(
                row['fince_cost_curr'] / sum(filter(None, [
                 row['debt_security_curr'], row['borrowings_curr'], row['dep_curr'], row['subordinated_liab_curr']]))
            )]))
            if row['form_id'] in [37, 38] and row['loan_curr'] not in [0, None] and row['intreset_inc_curr'] is not None
            and row['fince_cost_curr'] is not None and sum(filter(None, [
                row['debt_security_curr'], row['borrowings_curr'], row['dep_curr'], row['subordinated_liab_curr']
            ])) != 0 else None, axis=1
        )

        fs_data['spread_prev'] = fs_data.apply(
            lambda row: sum(filter(None, [(row['intreset_inc_prev'] / row['loan_prev']), -(
                row['fince_cost_prev'] / sum(filter(None, [
                 row['debt_security_prev'], row['borrowings_prev'], row['dep_prev'], row['subordinated_liab_prev']]))
            )]))
            if row['form_id'] in [37, 38] and row['loan_prev'] not in [0, None] and row['intreset_inc_prev'] is not None
            and row['fince_cost_prev'] is not None and sum(filter(None, [
                row['debt_security_prev'], row['borrowings_prev'], row['dep_prev'], row['subordinated_liab_prev']
            ])) != 0 else None, axis=1
        )

        fs_data['nim'] = fs_data.apply(lambda row: sum(filter(None, [
            row['intreset_inc_curr'], -row['fince_cost_curr'] if row['fince_cost_curr'] else None
        ])) / row['loan_curr'] if row['form_id'] in [37, 38] and row['loan_curr'] not in [0, None] else None, axis=1)

        fs_data['nim_prev'] = fs_data.apply(lambda row: sum(filter(None, [
            row['intreset_inc_prev'], -row['fince_cost_prev'] if row['fince_cost_prev'] else None
        ])) / row['loan_prev'] if row['form_id'] in [37, 38] and row['loan_prev'] not in [0, None] else None, axis=1)

        fs_data['leverage'] = fs_data.apply(lambda row: (
            (row['t_tot_fin_liab_curr'] / row['networth_curr']) if row['form_id'] in [37, 38] else
            (sum(filter(None, [row['lng_term_borrow_curr'], row['shrt_term_borrow_curr']])) / row['networth_curr'])
        ) if row['networth_curr'] not in [0, None] else None, axis=1)

        fs_data['yield'] = fs_data.apply(
            lambda row: row['intreset_inc_curr'] / row['loan_curr'] if row['loan_curr'] not in [0, None] else None,
            axis=1)

        fs_data['yield_prev'] = fs_data.apply(
            lambda row: row['intreset_inc_prev'] / row['loan_prev'] if row['loan_prev'] not in [0, None] else None,
            axis=1)

        fs_data['cost_of_funds'] = fs_data.apply(lambda row: (
            row['fince_cost_curr'] / row['loan_curr'] if row['loan_curr'] not in [0, None] else None
        ) if row['form_id'] in [37, 38] else (
            row['fince_cost_curr'] / sum(filter(None, [row['lng_term_borrow_curr'], row['shrt_term_borrow_curr']]))
            if sum(filter(None, [row['lng_term_borrow_curr'], row['shrt_term_borrow_curr']])) != 0 else None), axis=1)

        fs_data['cost_of_funds_prev'] = fs_data.apply(lambda row: (
            row['fince_cost_prev'] / row['loan_prev'] if row['loan_prev'] not in [0, None] else None
        ) if row['form_id'] in [37, 38] else (
            row['fince_cost_prev'] / sum(filter(None, [row['lng_term_borrow_prev'], row['shrt_term_borrow_prev']]))
            if sum(filter(None, [row['lng_term_borrow_prev'], row['shrt_term_borrow_prev']])) != 0 else None), axis=1)

        fs_data['credit_costs'] = fs_data.apply(
            lambda row: row['imp_loa_ratio_curr'] if row['form_id'] in [37, 38] else None, axis=1
        )

        fs_data['pat'] = fs_data['t_tot_profit_loss_for_period_curr'].astype('float')
        fs_data['other_expenses'] = fs_data['t_othr_exp_curr']
        fs_data['cash'] = fs_data['t_cash_eqv_bank_curr']
        fs_data['fixed_assets'] = fs_data['t_tot_fixed_assets_curr']

        fs_data['total_assets_change'] = fs_data.apply(lambda row: sum(filter(None, [
            row['total_assets_cur'], -row['total_assets_prev']
        ])) / row['total_assets_prev'] if row['total_assets_prev'] not in [0, None] else None, axis=1)

        fs_data['networth_change'] = fs_data.apply(lambda row: sum(filter(None, [
            row['networth_curr'], -row['networth_prev']
        ])) / row['networth_prev'] if row['networth_prev'] not in [0, None] else None, axis=1)

        fs_data['roe_change'] = fs_data.apply(lambda row: sum(filter(None, [
            row['roe_curr'], -row['roe_prev']])) / row['roe_prev'] if row['roe_prev'] not in [0, None] else None, axis=1)

        fs_data['spread_change'] = fs_data.apply(lambda row: sum(filter(None, [
            row['spread'], -row['spread_prev']
        ])) / row['spread_prev'] if row['spread_prev'] not in [0, None] else None, axis=1)

        fs_data['yield_change'] = fs_data.apply(lambda row: sum(filter(None, [
            row['yield'], -row['yield_prev']
        ])) / row['yield_prev'] if row['yield_prev'] not in [0, None] else None, axis=1)

        fs_data['cost_of_funds_change'] = fs_data.apply(lambda row: sum(filter(None, [
            row['cost_of_funds'], -row['cost_of_funds_prev']
        ])) / row['cost_of_funds_prev'] if row['cost_of_funds_prev'] not in [0, None] else None, axis=1)

        fs_data['credit_costs_change'] = fs_data.apply(lambda row: sum(filter(None, [
            row['imp_loa_ratio_curr'], -row['imp_loa_ratio_prev']
        ])) / row['imp_loa_ratio_prev'] if row['imp_loa_ratio_prev'] not in [0, None] else None, axis=1)

        fs_data['pat_change'] = fs_data.apply(lambda row: sum(filter(None, [
            row['t_tot_profit_loss_for_period_curr'], -row['t_tot_profit_loss_for_period_prev']
        ])) / row['t_tot_profit_loss_for_period_prev'] if row['t_tot_profit_loss_for_period_prev'] not in [0, None]
         else None, axis=1)

        fs_data['other_expenses_change'] = fs_data.apply(lambda row: sum(filter(None, [
            row['t_othr_exp_curr'], -row['t_othr_exp_prev']
        ])) / row['t_othr_exp_prev'] if row['t_othr_exp_prev'] not in [0, None] else None, axis=1)

        fs_data['cash_change'] = fs_data.apply(lambda row: sum(filter(None, [
            row['t_cash_eqv_bank_curr'], -row['t_cash_eqv_bank_prev']
        ])) / row['t_cash_eqv_bank_prev'] if row['t_cash_eqv_bank_prev'] not in [0, None] else None, axis=1)

        fs_data['fixed_assets_change'] = fs_data.apply(lambda row: sum(filter(None, [
            row['t_tot_fixed_assets_curr'], -row['t_tot_fixed_assets_prev']
        ])) / row['t_tot_fixed_assets_prev'] if row['t_tot_fixed_assets_prev'] not in [0, None] else None, axis=1)
        ################################################################################################################
        feature_cols = ['company_master_id', 'financial_year', 'total_assets_cur', 'networth_curr', 'roa', 'roe_curr',
                        'spread',
                        'nim', 'leverage', 'yield', 'cost_of_funds', 'credit_costs', 'pat',
                        'other_expenses', 'cash', 'fixed_assets', 'total_assets_change', 'networth_change',
                        'roe_change',
                        'spread_change', 'yield_change', 'cost_of_funds_change', 'credit_costs_change',
                        'pat_change', 'other_expenses_change', 'cash_change', 'fixed_assets_change', 'roe']

        # def replace_infinity(col, fill_value=None):
        #     return col.apply(lambda x: fill_value if x == np.inf or x == -np.inf else x)

        fs_data = fs_data[feature_cols]
        for col_name in feature_cols:
            # fs_data[col_name] = fs_data[col_name].apply(replace_infinity, None)
            fs_data[col_name] = fs_data[col_name].apply(lambda x: None if x == np.inf or x == -np.inf else x)

        fs_data['financial_year_start'] = pd.to_numeric(fs_data['financial_year'].str.split('-').str[0], errors='coerce')

        self.logger.info('Completed extracting financials for FS Companies')

        return fs_data

    def get_fs_financials(self):
        self.logger.info('started fetching data to calculate financials for FS Companies')
        etl_etl_data_connection = utils.get_db_connection(connection_name='etl_etl_data')

        bs_cols = ['accord_mca_balance_sheet_dtls_id', 'company_master_id', 'financial_year', 't_tot_assets_prev',
                   'is_consolidated_standalone_stmt', 't_tot_assets_curr', 'form_id', 'tot_assets_curr',
                   'tot_assets_prev', 't_networth_curr', 'dep_curr', 't_tot_share_hold_fund_curr', 't_networth_prev',
                   't_tot_share_hold_fund_prev', 'loan_curr', 'subordinated_liab_curr', 'borrowings_curr', 'loan_prev',
                   'dep_prev', 'debt_security_curr', 'debt_security_prev', 'borrowings_prev', 'subordinated_liab_prev',
                   'lng_term_borrow_curr', 'shrt_term_borrow_curr', 'lng_term_borrow_prev', 'shrt_term_borrow_prev',
                   't_tot_fin_liab_curr', 't_cash_eqv_bank_curr', 't_tot_fixed_assets_curr', 't_cash_eqv_bank_prev',
                   't_tot_fixed_assets_prev']

        pnl_cols = ['accord_mca_profit_loss_id', 'company_master_id', 'financial_year', 'form_id', 't_othr_exp_curr',
                    'is_consolidated_standalone_stmt', 'fince_cost_prev', 't_tot_profit_loss_for_period_curr',
                    't_tot_profit_loss_for_period_prev', 'intreset_inc_curr', 'fince_cost_curr', 'intreset_inc_prev',
                    't_othr_exp_prev']

        fin_ratio_cols = ['company_master_id', 'financial_year', 'is_consolidated_standalone_stmt', 'form_id',
                          'imp_loa_ratio_curr', 'financial_ratios_id', 'imp_loa_ratio_prev']

        accord_mca_balance_sheet_query = generate_query(
            table_name='accord_mca_balance_sheet',
            select_columns=bs_cols,
            filter_columns={'company_master_id': 'int', 'financial_year': 'str'},
            filter_values={'company_master_id': self.companies, 'financial_year': [self.as_on_dt_fy]}
        )

        accord_mca_balance_sheet = pd.read_sql(accord_mca_balance_sheet_query, etl_etl_data_connection)

        accord_mca_profit_loss_query = generate_query(
            table_name='accord_mca_profit_loss',
            select_columns=pnl_cols,
            filter_columns={'company_master_id': 'int', 'financial_year': 'str'},
            filter_values={'company_master_id': self.companies, 'financial_year': [self.as_on_dt_fy]}
        )

        accord_mca_profit_loss = pd.read_sql(accord_mca_profit_loss_query, etl_etl_data_connection)

        financial_ratios_query = generate_query(
            table_name='financial_ratios',
            select_columns=fin_ratio_cols,
            filter_columns={'company_master_id': 'int', 'financial_year': 'str'},
            filter_values={'company_master_id': self.companies, 'financial_year': [self.as_on_dt_fy]}
        )

        financial_ratios = pd.read_sql(financial_ratios_query, etl_etl_data_connection)
        self.logger.info('Completed fetching data to calculate financials for FS Companies')

        return self.extract_financial(accord_mca_balance_sheet, accord_mca_profit_loss, financial_ratios)
