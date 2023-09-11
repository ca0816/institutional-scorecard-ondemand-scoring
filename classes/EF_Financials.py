#  Author : Naga Satish Chitturi(CA0316)
#  Created : 2023-08-20
#  Last Modified : 2020-02-23
#  Description : This class contains related to EnterpriseFinanceCompanies Financial features(KPIs).
#
#  Change Log
#  ---------------------------------------------------------------------------
#   Date			Author			Comment
#  ---------------------------------------------------------------------------
#

import logging
from datetime import datetime

import pandas as pd

from classes import Utils, get_logger, generate_query

utils = Utils()

final_features_features = [
    'company_master_id', 'financial_year', 'financial_year_start',

    'total_debt_by_pat', 'current_ratio', 'quick_ratio', 'pat', 'ebitda', 'ebitda_by_sales',

    'inventory_days_diff_1_yrs', 'inventory_days_diff_2_yrs', 'inventory_days_diff_3_yrs', 'inventory_days_diff_4_yrs',

    'receivables_days_diff_1_yrs', 'receivables_days_diff_2_yrs', 'receivables_days_diff_3_yrs',
    'receivables_days_diff_4_yrs',

    'networth_diff_perc_1_yrs', 'networth_diff_perc_2_yrs', 'networth_diff_perc_3_yrs', 'networth_diff_perc_4_yrs',

    'pat_diff_perc_1_yrs', 'pat_diff_perc_2_yrs', 'pat_diff_perc_3_yrs', 'pat_diff_perc_4_yrs',

    'ebitda_by_sales_diff_1_yrs', 'ebitda_by_sales_diff_2_yrs', 'ebitda_by_sales_diff_3_yrs',
    'ebitda_by_sales_diff_4_yrs',

    'working_capital_by_asset', 'short_term_debt_by_cash_eqv', 'short_term_debt_by_networth', 'roa', 'roe',
    'working_capital_turnover_ratio',

    'tot_fixed_asset_diff_perc_1_yrs', 'tot_fixed_asset_diff_perc_2_yrs', 'tot_fixed_asset_diff_perc_3_yrs',
    'tot_fixed_asset_diff_perc_4_yrs',

    'roa_diff_1_yrs', 'roa_diff_2_yrs', 'roa_diff_3_yrs', 'roa_diff_4_yrs', 'roe_diff_1_yrs', 'roe_diff_2_yrs',
    'roe_diff_3_yrs', 'roe_diff_4_yrs', 'total_revenue_diff_perc_1_yrs', 'total_revenue_diff_perc_2_yrs',
    'total_revenue_diff_perc_3_yrs', 'total_revenue_diff_perc_4_yrs',

    'revenue_from_operations_diff_perc_1_yrs', 'revenue_from_operations_diff_perc_2_yrs',
    'revenue_from_operations_diff_perc_3_yrs', 'revenue_from_operations_diff_perc_4_yrs',
    'growth_in_operating_income', 'operating_income', 'roce', 'debt_by_gross_accrual', 'ltborrowing_by_gross_accrual',
    'ltborrowing_by_ebitda', 'debt_by_ebitda', 'creditor_period', 'working_capital_cycle', 'ebidta_margins_per',
    'pat_margins_per', 'inventory_days', 'receivables_days'
]


class EfFinancialFeatures:
    def __init__(self, companies_list: list, as_on_date: datetime.date):
        """
        Class to generate Financial Features of Enterprise Companies
        :param companies_list: list of companies to extract features
        :param as_on_date: A date on which features should be extracted for the given set of companies.
        """
        self.logger = get_logger('EfFinancialsLogger', logging.INFO)
        self.companies = companies_list
        self.as_on_date = as_on_date
        self.end_year = self.as_on_date.year - 1 if self.as_on_date.month <= 3 else self.as_on_date.year
        self.start_year = self.end_year - 4
        as_on_dt_fy = f"{self.as_on_date.year - 2}-{self.as_on_date.year - 1}" if self.as_on_date.month <= 3 else f"{self.as_on_date.year - 1}-{self.as_on_date.year}"

        self.as_on_dt_fy = as_on_dt_fy
        self.valid_years = [f'{i}-{i + 1}' for i in range(self.start_year, self.end_year)]

    def extract_financial(self, bs: pd.DataFrame, pnl: pd.DataFrame, financial_ratio: pd.DataFrame):
        """
        Function to extract Financial features for Enterprise Companies.
        :param bs: dataframe with Accord MCA Balance Sheet data
        :param pnl: dataframe with Accord MCA Profit Loss data
        :param financial_ratio: dataframe with Accord MCA Financial Ratios data
        :return: DataFrame
        """
        self.logger.info('Started extracting financials for EF Companies')

        bs['is_consolidated_standalone_stmt'] = bs['is_consolidated_standalone_stmt'].str.lower().str.strip()

        bs['comp_networth'] = bs['share_cap_curr'] + bs['rsrv_surp_curr'] + bs['money_share_warrnt_curr']

        bs['t_tot_non_curr_liability_curr_'] = bs.apply(
            lambda row: 0 if pd.isnull(row['t_tot_non_curr_liability_curr']) else row['t_tot_non_curr_liability_curr'],
            axis=1)

        bs['t_tot_curr_liability_curr_'] = bs.apply(
            lambda row: 0 if pd.isnull(row['t_tot_curr_liability_curr']) else row['t_tot_curr_liability_curr'], axis=1)

        bs['total_liabilities'] = bs['t_tot_non_curr_liability_curr_'] + bs['t_tot_curr_liability_curr_']
        bs['comp_networth_2'] = bs['t_tot_share_hold_fund_curr'] + bs["t_tot_share_hold_fund_prev"]

        bs = bs.fillna(value={'comp_networth_2': 0, 'comp_networth': 0})
        bs['comp_networth'] = bs['comp_networth'].combine_first(bs['comp_networth_2'])
        bs['networth'] = bs['comp_networth']

        bs['document_date'] = pd.to_datetime(bs['document_date'], format='%Y-%m-%d')
        ################################################################################################################
        pnl['is_consolidated_standalone_stmt'] = pnl['is_consolidated_standalone_stmt'].str.lower().str.strip()

        pnl['document_date'] = pd.to_datetime(pnl['document_date'], format='%Y-%m-%d')
        ################################################################################################################
        financial_ratio['is_consolidated_standalone_stmt'] = financial_ratio[
            'is_consolidated_standalone_stmt'].str.lower().str.strip()

        financial_ratio['document_date'] = pd.to_datetime(financial_ratio['document_date'], format='%Y-%m-%d')
        ################################################################################################################
        # fetch the latest record for each financial year
        bs_window = bs.sort_values(by='document_date', ascending=False).groupby(
            ['company_master_id', 'financial_year', 'is_consolidated_standalone_stmt'], group_keys=True)[
            'company_master_id'].rank(method='first', ascending=False)
        bs['id1'] = bs_window
        bs = bs[(bs['id1'] == 1)]
        bs = bs.drop(columns=['id1', 'document_date'])

        pnl_window = pnl.sort_values(by='document_date', ascending=False).groupby(
            ['company_master_id', 'financial_year', 'is_consolidated_standalone_stmt'], group_keys=True)[
            'company_master_id'].rank(method='first', ascending=False)
        pnl['id1'] = pnl_window
        pnl = pnl[(pnl['id1'] == 1)]
        pnl = pnl.drop(columns=['id1', 'document_date'])

        financial_ratio_window = financial_ratio.sort_values(by='document_date', ascending=False).groupby(
            ['company_master_id', 'financial_year', 'is_consolidated_standalone_stmt'], group_keys=True)[
            'company_master_id'].rank(method='first', ascending=False)
        financial_ratio['id1'] = financial_ratio_window
        financial_ratio = financial_ratio[(financial_ratio['id1'] == 1)]
        financial_ratio = financial_ratio.drop(columns=['id1', 'document_date'])

        # join bs, pnl, ratios
        bs_pnl = pd.merge(bs, pnl, on=['financial_year', 'is_consolidated_standalone_stmt', 'company_master_id'])
        bs_pnl_ratios = pd.merge(bs_pnl, financial_ratio,
                                 on=['financial_year', 'is_consolidated_standalone_stmt', 'company_master_id'])

        bs_pnl_ratios['preference_order'] = bs_pnl_ratios.apply(
            lambda row: 0 if row['is_consolidated_standalone_stmt'] == 'standalone' else 1, axis=1
        )

        pref_win = bs_pnl_ratios.sort_values(by='preference_order', ascending=False).groupby(
            ['company_master_id', 'financial_year'], group_keys=True)['company_master_id'].rank(
            method='first', ascending=False)
        bs_pnl_ratios['id2'] = pref_win
        bs_pnl_ratios = bs_pnl_ratios[(bs_pnl_ratios['id2'] == 1)]
        bs_pnl_ratios = bs_pnl_ratios.drop(columns=['preference_order', 'id2'])

        bs_pnl_ratios['financial_year_start'] = pd.to_numeric(bs_pnl_ratios['financial_year'].str.split('-').str[0],
                                                              errors='coerce')
        ################################################################################################################
        bs_pnl_ratios['est_rev'] = bs_pnl_ratios['t_rev_frm_prod_sale_curr'] + bs_pnl_ratios['t_rev_frm_serv_sale_curr']
        bs_pnl_ratios['total_revenue'] = bs_pnl_ratios['t_tot_rev_curr']
        bs_pnl_ratios['pat'] = bs_pnl_ratios['tot_profit_bfr_tax_curr'] - bs_pnl_ratios['tot_tax_exp_curr']
        bs_pnl_ratios['pat_prev'] = bs_pnl_ratios['t_tot_profit_bfr_tax_prev'] - bs_pnl_ratios['tot_tax_exp_prev']
        bs_pnl_ratios['growth_in_pat'] = (bs_pnl_ratios['pat'] - bs_pnl_ratios['pat_prev']) / bs_pnl_ratios['pat_prev']

        bs_pnl_ratios['ebitda'] = bs_pnl_ratios['t_editda_curr']
        bs_pnl_ratios['ebitda_by_sales'] = bs_pnl_ratios['t_editda_curr'] / bs_pnl_ratios['gross_sales_curr']
        bs_pnl_ratios['ebitda_margin'] = bs_pnl_ratios['t_editda_curr'] / bs_pnl_ratios['t_tot_rev_curr']
        bs_pnl_ratios['total_liabilities'] = bs_pnl_ratios['t_tot_non_curr_liability_curr'] + bs_pnl_ratios[
            't_tot_curr_liability_curr']

        bs_pnl_ratios['working_capital'] = bs_pnl_ratios['tot_curr_assets_curr'] - bs_pnl_ratios[
            'tot_curr_liability_curr']

        bs_pnl_ratios['working_capital_by_asset'] = bs_pnl_ratios['working_capital'] / bs_pnl_ratios['tot_assets_curr']
        bs_pnl_ratios['short_term_debt_by_cash_eqv'] = bs_pnl_ratios['shrt_term_borrow_curr'] / bs_pnl_ratios[
            'cash_eqv_curr']
        bs_pnl_ratios['short_term_debt_by_networth'] = bs_pnl_ratios['shrt_term_borrow_curr'] / bs_pnl_ratios[
            'networth']
        bs_pnl_ratios['roa'] = bs_pnl_ratios['pat'] / bs_pnl_ratios['t_tot_assets_curr']
        bs_pnl_ratios['roe'] = bs_pnl_ratios['pat'] / bs_pnl_ratios['networth']

        bs_pnl_ratios['total_borrow'] = bs_pnl_ratios['lng_term_borrow_curr'] + bs_pnl_ratios['shrt_term_borrow_curr']
        bs_pnl_ratios['total_debt_by_pat'] = bs_pnl_ratios['total_borrow'] / bs_pnl_ratios['pat']

        print(bs_pnl_ratios.to_string())
        bs_pnl_ratios['pat_by_ebitda'] = bs_pnl_ratios['pat'] / bs_pnl_ratios['t_editda_curr']

        bs_pnl_ratios['revenue_from_operations'] = bs_pnl_ratios['t_tot_rev_frm_operation_curr']
        bs_pnl_ratios['tot_fixed_asset'] = bs_pnl_ratios['t_tot_fixed_assets_curr']

        bs_pnl_ratios['growth_in_operating_income'] = (
                                                              ((bs_pnl_ratios['t_editda_curr'] - bs_pnl_ratios[
                                                                  'tot_depr_depl_amor_curr']) /
                                                               (bs_pnl_ratios['t_editda_prev'] - bs_pnl_ratios[
                                                                   'tot_depr_depl_amor_prev'])) - 1
                                                      ) * 100

        bs_pnl_ratios['operating_income'] = bs_pnl_ratios['t_editda_curr'] - bs_pnl_ratios['tot_depr_depl_amor_curr']
        bs_pnl_ratios['roce'] = bs_pnl_ratios['return_on_cap_employed']
        bs_pnl_ratios['pat_by_ebitda'] = bs_pnl_ratios['pat'] / bs_pnl_ratios['t_editda_curr']

        bs_pnl_ratios['debt_by_gross_accrual'] = bs_pnl_ratios['total_borrow'] / (
                bs_pnl_ratios['t_tot_profit_loss_for_period_curr'] + bs_pnl_ratios['tot_depr_depl_amor_curr'])
        bs_pnl_ratios['ltborrowing_by_gross_accrual'] = bs_pnl_ratios['lng_term_borrow_curr'] / (
                bs_pnl_ratios['t_tot_profit_loss_for_period_curr'] + bs_pnl_ratios['tot_depr_depl_amor_curr'])

        bs_pnl_ratios['ltborrowing_by_ebitda'] = bs_pnl_ratios['lng_term_borrow_curr'] / bs_pnl_ratios['ebitda']
        bs_pnl_ratios['debt_by_ebitda'] = bs_pnl_ratios['total_borrow'] / bs_pnl_ratios['ebitda']
        bs_pnl_ratios['working_capital_turnover_ratio'] = bs_pnl_ratios['revenue_from_operations'] / bs_pnl_ratios[
            'working_capital']

        bs_pnl_ratios['creditor_period'] = (bs_pnl_ratios['trade_pay_curr'] * 365) / (
                bs_pnl_ratios['t_invent_chng_fg_wip_stck_in_trade_curr'] + bs_pnl_ratios['material_cost_consumd_curr'] +
                bs_pnl_ratios['invent_stck_in_trade_curr'])

        bs_pnl_ratios['working_capital_cycle'] = (bs_pnl_ratios['inventory_days'] + bs_pnl_ratios['receivables_days'] -
                                                  bs_pnl_ratios['creditor_period'])

        time_features = ['inventory_days', 'receivables_days', 'networth', 'pat', 'ebitda', 'ebitda_by_sales', 'roa',
                         'roe', 'ebitda_margin', 'tot_fixed_asset', 'revenue_from_operations', 'total_revenue']

        grouped_data_by_year = bs_pnl_ratios.sort_values(by=['financial_year_start'], ascending=[False]).groupby(
            by=['company_master_id'], group_keys=True)

        for feat in time_features:
            for i in range(1, 5):
                prv_yrs_value_column = grouped_data_by_year[feat].shift(-i)
                # Rename the shifted column to '{feat}_prv_{i}_yrs' and Concatenate to 'bs_pnl_ratios'
                bs_pnl_ratios = pd.concat(
                    [bs_pnl_ratios, prv_yrs_value_column.rename(f'{feat}_prv_{i}_yrs', inplace=True)], axis=1
                )
                if feat != 'total_revenue':
                    diff_wrt_to_prv_yr_column = bs_pnl_ratios[feat] - bs_pnl_ratios[f'{feat}_prv_{i}_yrs']
                    bs_pnl_ratios = pd.concat(
                        [bs_pnl_ratios, diff_wrt_to_prv_yr_column.rename(f'{feat}_diff_{i}_yrs', inplace=True)], axis=1)
                    perc_diff_wrt_prv_yrs = 100 * (
                            bs_pnl_ratios[f'{feat}_diff_{i}_yrs'] / bs_pnl_ratios[f'{feat}_prv_{i}_yrs'])

                    bs_pnl_ratios = pd.concat(
                        [bs_pnl_ratios, perc_diff_wrt_prv_yrs.rename(f'{feat}_diff_perc_{i}_yrs', inplace=True)], axis=1
                    )
                else:
                    bs_pnl_ratios[f'{feat}_diff_perc_{i}_yrs'] = bs_pnl_ratios[feat] / bs_pnl_ratios[
                        f'{feat}_prv_{i}_yrs']
                    bs_pnl_ratios[f'{feat}_diff_perc_{i}_yrs'] = bs_pnl_ratios.apply(
                        lambda row: (row[f'{feat}_diff_perc_{i}_yrs'] ** (1 / i)) - 1
                        if row[f'{feat}_diff_perc_{i}_yrs'] > 0 else None, axis=1
                    )
        ################################################################################################################
        bs_pnl_ratios = bs_pnl_ratios[
            bs_pnl_ratios['financial_year'] == self.as_on_dt_fy
            ]
        bs_pnl_ratios = bs_pnl_ratios[final_features_features]

        self.logger.info('Completed extracting financials for EF Companies')
        return bs_pnl_ratios

    def get_ef_financials(self):
        self.logger.info('Started fetching data to calculate financials for EF Companies')
        etl_etl_data_connection = utils.get_db_connection(connection_name='etl_etl_data')

        bs_cols = ['company_master_id', 'financial_year', 'is_consolidated_standalone_stmt', 'tot_curr_assets_curr',
                   'tot_curr_liability_curr', 'share_cap_curr', 'rsrv_surp_curr', 'money_share_warrnt_curr',
                   'document_date',
                   't_tot_non_curr_liability_curr', 't_tot_curr_liability_curr', 't_tot_share_hold_fund_curr',
                   'cash_eqv_curr',
                   'tot_assets_curr', 't_tot_share_hold_fund_prev', 'shrt_term_borrow_curr', 't_tot_assets_curr',
                   'lng_term_borrow_curr', 't_tot_fixed_assets_curr', 'trade_pay_curr']
        pnl_cols = ['company_master_id', 'financial_year', 'is_consolidated_standalone_stmt', 'tot_profit_bfr_tax_curr',
                    't_tot_rev_frm_operation_curr', 'document_date', 't_rev_frm_serv_sale_curr', 'tot_tax_exp_curr',
                    't_editda_prev', 't_tot_profit_bfr_tax_prev', 'tot_tax_exp_prev', 't_editda_curr', 't_tot_rev_curr',
                    'gross_sales_curr', 'tot_depr_depl_amor_curr', 'tot_depr_depl_amor_prev',
                    't_rev_frm_prod_sale_curr',
                    't_tot_profit_loss_for_period_curr', 'invent_stck_in_trade_curr', 'material_cost_consumd_curr',
                    't_invent_chng_fg_wip_stck_in_trade_curr', ]
        fin_ratio_cols = ['company_master_id', 'financial_year', 'is_consolidated_standalone_stmt', 'inventory_days',
                          'document_date', 'return_on_cap_employed', 'receivables_days', 'current_ratio', 'quick_ratio',
                          'ebidta_margins_per', 'pat_margins_per']

        accord_mca_balance_sheet_query = generate_query(
            table_name='accord_mca_balance_sheet',
            select_columns=bs_cols,
            filter_columns={'company_master_id': 'int', 'financial_year': 'str'},
            filter_values={'company_master_id': self.companies, 'financial_year': self.valid_years}
        )

        accord_mca_balance_sheet = pd.read_sql(accord_mca_balance_sheet_query, etl_etl_data_connection)

        accord_mca_profit_loss_query = generate_query(
            table_name='accord_mca_profit_loss',
            select_columns=pnl_cols,
            filter_columns={'company_master_id': 'int', 'financial_year': 'str'},
            filter_values={'company_master_id': self.companies, 'financial_year': self.valid_years}
        )

        accord_mca_profit_loss = pd.read_sql(accord_mca_profit_loss_query, etl_etl_data_connection)

        accord_mca_financial_ratio_query = generate_query(
            table_name='accord_mca_financial_ratio',
            select_columns=fin_ratio_cols,
            filter_columns={'company_master_id': 'int', 'financial_year': 'str'},
            filter_values={'company_master_id': self.companies, 'financial_year': self.valid_years}
        )

        accord_mca_financial_ratio = pd.read_sql(accord_mca_financial_ratio_query, etl_etl_data_connection)
        self.logger.info('Completed fetching data to calculate financials for EF Companies')

        return self.extract_financial(accord_mca_balance_sheet, accord_mca_profit_loss, accord_mca_financial_ratio)
