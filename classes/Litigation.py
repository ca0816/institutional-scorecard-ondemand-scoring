#  Author : Naga Satish Chitturi(CA0316)
#  Created : 2023-08-20
#  Last Modified : 2020-02-23
#  Description : This class contains related to Litigation features(KPIs).
#
#  Change Log
#  ---------------------------------------------------------------------------
#   Date			Author			Comment
#  ---------------------------------------------------------------------------
#

# Standard library imports
import logging
from datetime import datetime

# Third party module imports
import pandas as pd
from pyspark.sql import functions as f, Window

# Project module imports
from classes import Utils
from classes.Utilities import get_logger, generate_query

utils = Utils()


class LitigationFeatures:
    def __init__(self, companies_list: list, as_on_date: datetime.date):
        """
        Class to generate Litigation Features.
        :param companies_list: list of companies to extract features.
        :param as_on_date: A date on which features should be extracted for the given set of companies.
        """
        self.logger = get_logger('LitigationFeaturesLogger', logging.INFO)
        self.companies = companies_list
        self.as_on_date = pd.to_datetime(as_on_date)
        self.start_date = pd.to_datetime('2017-01-01', format='%Y-%m-%d').date()

    def sat_cases(self, sat_court: pd.DataFrame):
        self.logger.info("Started Extracting sat cases")
        sat_court['d_date'] = pd.to_datetime(sat_court['d_date'])
        sat_court_df = sat_court[sat_court['d_date'] > pd.to_datetime('1900-01-01', format='%Y-%m-%d')]

        if self.as_on_date.date() < pd.Timestamp.now().date():
            sat_court_df = sat_court[sat_court['d_date'] <= self.as_on_date]

        sat_court_cases = sat_court_df.groupby(['company_master_id']).agg({'link': 'nunique'}).reset_index()

        sat_court_cases = sat_court_cases.rename({'link': 'number_of_past_sat_cases'})

        return sat_court_cases

    def bifr_cases(self, bifr: pd.DataFrame):
        self.logger.info("Started Extracting bifr cases")

        disposed_status = ['Abated', 'Dismissed as Non Maintainable', 'Dropped (N/W Positive)',
                           'Winding up Reccomended /Confirmed', 'Winding up Notice']

        bifr['is_disposed'] = bifr.apply(lambda row: True if row['status'] in disposed_status else False, axis=1)
        bifr['order_date'] = bifr.apply(lambda row: str(row['case_year']) + '-01-01', axis=1)
        bifr['order_date'] = pd.to_datetime(bifr['order_date'], format='%Y-%m-%d', errors='coerce')

        bifr['disposed_date'] = bifr.apply(lambda row: row['d_date_of_last_order'] if row['is_disposed'] else pd.NaT,
                                           axis=1)

        bifr['disposed_date'] = pd.to_datetime(bifr['disposed_date'], format='%Y-%m-%d')

        bifr['valid_record'] = bifr.apply(
            lambda row: True if row['disposed_date'] == pd.NaT else (
                True if (row['disposed_date'] >= pd.to_datetime('1900-01-01', format='%Y-%m-%d')) and
                        (row['disposed_date'] <= pd.to_datetime(pd.Timestamp.now().date())) and
                        (row['order_date'] >= pd.to_datetime('1900-01-01', format='%Y-%m-%d')) and
                        (row['order_date'] <= pd.to_datetime(pd.Timestamp.now().date())) and
                        (row['disposed_date'] < pd.to_datetime(pd.Timestamp.now().date())) else False
            ),
            axis=1)

        bifr = bifr[bifr['valid_record']]

        bifr = bifr[['company_master_id', 'case_number', 'order_date', 'disposed_date']]

        bifr_grouped = bifr.groupby(by=['company_master_id', 'case_number'], group_keys=True)

        bifr = bifr.assign(order_date=bifr_grouped['order_date'].transform(min))
        bifr = bifr.assign(disposed_date=bifr_grouped['disposed_date'].transform(max))

        bifr = bifr.drop_duplicates()
        print(bifr.to_string())

        exit(1)


        # bifr = bifr.withColumn('is_disposed', f.col('status').isin(disposed_status)) \
        #     .withColumn('order_date', f.to_date(f.concat(f.col('case_year'), f.lit('-01-01'))))

        # bifr = bifr.withColumn('disposed_date', f.when(f.col('is_disposed'), f.col('d_date_of_last_order'))) \
        #     .withColumn('valid_record', f.when(f.col("disposed_date").isNull(), True)
        #                 .when((f.col("disposed_date") >= '1900-01-01') &
        #                       (f.col("disposed_date") <= f.current_date()) &
        #                       (f.col('order_date') >= '1900-01-01') &
        #                       (f.col('order_date') <= f.current_date()) &
        #                       (f.col("disposed_date") < f.current_date()), True).otherwise(False)) \
        #     .filter(f.col('valid_record')).select('company_master_id', 'case_number', 'order_date', 'disposed_date')

        # window_bifr_unique_cases = Window.partitionBy('company_master_id', 'case_number')
        # bifr = bifr.withColumn('order_date', f.min('order_date').over(window_bifr_unique_cases)) \
        #     .withColumn('disposed_date', f.max('disposed_date').over(window_bifr_unique_cases)) \
        #     .withColumn('row', f.row_number().over(window_bifr_unique_cases.orderBy(f.lit(None)))) \
        #     .filter(f.col('row') == 1).drop('row')

        cases_filed_df = bifr.groupBy('company_master_id') \
            .agg(f.count('order_date').alias('number_of_cases_filed'))

        cases_closed_df = bifr.groupBy('company_master_id') \
            .agg(f.count('disposed_date').alias('number_of_cases_closed'))

        final_df = cases_filed_df.join(cases_closed_df, ['company_master_id'], 'outer')
        final_df = final_df \
            .withColumn('number_of_bifr_cases', f.col('number_of_cases_filed') - f.col('number_of_cases_closed')) \
            .select(f.col('company_master_id').astype('long'), f.current_date().alias('as_on_date'),
                    f.col('number_of_bifr_cases').astype('short'))

        self.logger.info("Completed Extracting bifr cases")

        # if run_date and datetime.datetime.strptime(run_date, '%Y-%m-%d').date() < datetime.datetime.now().date():
        #     bifr = bifr.withColumn('disposed_date', f.when(f.col('is_disposed'), f.col('d_date_of_last_order'))
        #                            .otherwise(f.current_date())) \
        #         .filter((f.col('order_date') >= '1900-01-01') & (f.col('order_date') <= f.current_date()) &
        #                 (f.col('disposed_date') >= '1900-01-01') & (f.col('disposed_date') <= f.current_date()) &
        #                 (f.col('order_date') <= f.col('disposed_date'))) \
        #         .select('company_master_id', 'case_number', 'order_date', 'disposed_date')
        #
        #     window_bifr_unique_cases = Window.partitionBy('company_master_id', 'case_number')
        #     bifr = bifr.withColumn('order_date', f.min('order_date').over(window_bifr_unique_cases)) \
        #         .withColumn('disposed_date', f.max('disposed_date').over(window_bifr_unique_cases)) \
        #         .withColumn('row', f.row_number().over(window_bifr_unique_cases.orderBy(f.lit(None)))) \
        #         .filter(f.col('row') == 1).drop('row')
        #
        #     _bifr_lit = bifr.filter((f.col('order_date') <= f.current_date()) &
        #                             (f.col('disposed_date') <= f.current_date()))
        #     cases_filed_df = _bifr_lit.groupBy('company_master_id', 'order_date') \
        #         .agg(f.count('case_number').alias('number_of_cases_filed')) \
        #         .withColumnRenamed('order_date', 'as_on_date')
        #
        #     cases_closed_df = _bifr_lit.groupBy('company_master_id', 'disposed_date') \
        #         .agg(f.count('case_number').alias('number_of_cases_closed')) \
        #         .withColumnRenamed('disposed_date', 'as_on_date')
        #
        #     final_df = cases_filed_df.join(cases_closed_df, ['company_master_id', 'as_on_date'], 'outer')
        #
        #     all_dates_df = _bifr_lit.select('company_master_id').distinct().withColumn('start_date', start_date) \
        #         .withColumn('end_date', f.current_date()) \
        #         .withColumn('as_on_date', f.explode(f.expr('sequence(start_date, end_date, interval 1 day)'))) \
        #         .select('company_master_id', 'as_on_date')
        #
        #     final_df = final_df.join(all_dates_df, ['company_master_id', 'as_on_date'], 'outer').fillna(
        #         {'number_of_cases_filed': 0, 'number_of_cases_closed': 0})
        #
        #     window = Window.partitionBy('company_master_id').orderBy('as_on_date').rowsBetween(Window.unboundedPreceding,
        #                                                                                        Window.currentRow)
        #     final_df = final_df \
        #         .withColumn('number_of_cases_filed', f.sum('number_of_cases_filed').over(window)) \
        #         .withColumn('number_of_cases_closed', f.sum('number_of_cases_closed').over(window))
        #
        #     final_df = final_df.withColumn('number_of_bifr_cases',
        #                                    f.col('number_of_cases_filed') - f.col('number_of_cases_closed')) \
        #         .filter(f.col('as_on_date') == run_date).select(f.col('company_master_id').astype('long'), 'as_on_date',
        #                                                         f.col('number_of_bifr_cases').astype('short'))
        #     self.logger.info("Completed Extracting bifr cases")
        #
        #     return final_df
        # else:
        #     bifr = bifr.withColumn('disposed_date', f.when(f.col('is_disposed'), f.col('d_date_of_last_order'))) \
        #         .withColumn('valid_record', f.when(f.col("disposed_date").isNull(), True)
        #                     .when((f.col("disposed_date") >= '1900-01-01') &
        #                           (f.col("disposed_date") <= f.current_date()) &
        #                           (f.col('order_date') >= '1900-01-01') &
        #                           (f.col('order_date') <= f.current_date()) &
        #                           (f.col("disposed_date") < f.current_date()), True).otherwise(False)) \
        #         .filter(f.col('valid_record')).select('company_master_id', 'case_number', 'order_date', 'disposed_date')
        #
        #     window_bifr_unique_cases = Window.partitionBy('company_master_id', 'case_number')
        #     bifr = bifr.withColumn('order_date', f.min('order_date').over(window_bifr_unique_cases)) \
        #         .withColumn('disposed_date', f.max('disposed_date').over(window_bifr_unique_cases)) \
        #         .withColumn('row', f.row_number().over(window_bifr_unique_cases.orderBy(f.lit(None)))) \
        #         .filter(f.col('row') == 1).drop('row')
        #
        #     cases_filed_df = bifr.groupBy('company_master_id') \
        #         .agg(f.count('order_date').alias('number_of_cases_filed'))
        #
        #     cases_closed_df = bifr.groupBy('company_master_id') \
        #         .agg(f.count('disposed_date').alias('number_of_cases_closed'))
        #
        #     final_df = cases_filed_df.join(cases_closed_df, ['company_master_id'], 'outer')
        #     final_df = final_df \
        #         .withColumn('number_of_bifr_cases', f.col('number_of_cases_filed') - f.col('number_of_cases_closed')) \
        #         .select(f.col('company_master_id').astype('long'), f.current_date().alias('as_on_date'),
        #                 f.col('number_of_bifr_cases').astype('short'))
        #
        #     self.logger.info("Completed Extracting bifr cases")
        #     return final_df
    #
    #
    # def nclt_cases(nclt_court: DataFrame, run_date: str = None):
    #     nclt_disposed_date = f.to_date(f.col('disposed_date'), 'dd-MM-yyyy')
    #
    #     nclt_court = nclt_court \
    #         .withColumn('case_status', f.trim(f.lower(f.col('case_status')))) \
    #         .withColumn('sections', f.lower(f.col('sections')))\
    #         .filter((f.col('sections') == 'ibc under sec 7') &
    #                 (f.col('case_status').isin(['disposed off', 'disposed', 'pending'])))
    #
    #     if run_date and datetime.datetime.strptime(run_date, '%Y-%m-%d').date() < datetime.datetime.now().date():
    #         nclt_court = nclt_court\
    #             .withColumn('disposed_date', f.when(nclt_disposed_date.isNotNull(), nclt_disposed_date)
    #                         .otherwise(f.current_date())) \
    #             .filter((f.col('d_order_date') <= f.current_date()) & (f.col('d_order_date') >= '1900-01-01') &
    #                     (f.col('disposed_date') <= f.current_date()) & (f.col('disposed_date') >= '1900-01-01') &
    #                     (f.col('d_order_date') <= f.col('disposed_date'))) \
    #             .select('company_master_id', 'cpno', 'd_order_date', 'disposed_date')
    #
    #         window_nclt_unique_cases = Window.partitionBy('company_master_id', 'cpno')
    #         nclt_court = nclt_court.withColumn('d_order_date', f.min('d_order_date').over(window_nclt_unique_cases)) \
    #             .withColumn('disposed_date', f.max('disposed_date').over(window_nclt_unique_cases)) \
    #             .withColumn('row', f.row_number().over(window_nclt_unique_cases.orderBy(f.lit(None)))) \
    #             .filter(f.col('row') == 1).drop('row')
    #
    #         cases_filed_df = nclt_court.groupBy('company_master_id', 'd_order_date') \
    #             .agg(f.count('cpno').alias('number_of_cases_filed')) \
    #             .withColumnRenamed('d_order_date', 'as_on_date')
    #
    #         cases_closed_df = nclt_court.groupBy('company_master_id', 'disposed_date') \
    #             .agg(f.count('cpno').alias('number_of_cases_closed')) \
    #             .withColumnRenamed('disposed_date', 'as_on_date')
    #
    #         final_df = cases_filed_df.join(cases_closed_df, ['company_master_id', 'as_on_date'], 'outer')
    #
    #         all_dates_df = nclt_court.select('company_master_id').distinct().withColumn('start_date', start_date) \
    #             .withColumn('end_date', f.lit(f.current_date())) \
    #             .withColumn('as_on_date', f.explode(f.expr('sequence(start_date, end_date, interval 1 day)'))) \
    #             .select('company_master_id', 'as_on_date')
    #
    #         final_df = final_df.join(all_dates_df, ['company_master_id', 'as_on_date'], 'outer').fillna(
    #             {'number_of_cases_filed': 0, 'number_of_cases_closed': 0})
    #
    #         window = Window.partitionBy('company_master_id').orderBy('as_on_date').rowsBetween(Window.unboundedPreceding,
    #                                                                                            Window.currentRow)
    #
    #         final_df = final_df.withColumn('number_of_cases_filed', f.sum('number_of_cases_filed').over(window)) \
    #             .withColumn('number_of_cases_closed', f.sum('number_of_cases_closed').over(window))
    #
    #         final_df = final_df \
    #             .withColumn('number_of_nclt_cases', f.col('number_of_cases_filed') - f.col('number_of_cases_closed')) \
    #             .filter(f.col('as_on_date') == run_date).select(f.col('company_master_id').astype('long'), 'as_on_date',
    #                                                             f.col('number_of_nclt_cases').astype('short'))
    #         return final_df
    #     else:
    #         nclt_court = nclt_court\
    #             .withColumn('disposed_date', f.when(nclt_disposed_date.isNotNull(), nclt_disposed_date))\
    #             .withColumn('valid_record', f.when(f.col('disposed_date').isNull(), True)
    #                         .when((f.col('disposed_date') >= '1900-01-01') &
    #                               (f.col('disposed_date') <= f.current_date()) &
    #                               (f.col('d_order_date') < f.col('disposed_date')) &
    #                               (f.col('d_order_date') >= '1900-01-01') &
    #                               (f.col('d_order_date') <= f.current_date()), True).otherwise(False)) \
    #             .filter(f.col('valid_record')).select('company_master_id', 'cpno', 'd_order_date', 'disposed_date')
    #
    #         window_nclt_unique_cases = Window.partitionBy('company_master_id', 'cpno')
    #         nclt_court = nclt_court.withColumn('d_order_date', f.min('d_order_date').over(window_nclt_unique_cases)) \
    #             .withColumn('disposed_date', f.max('disposed_date').over(window_nclt_unique_cases)) \
    #             .withColumn('row', f.row_number().over(window_nclt_unique_cases.orderBy(f.lit(None)))) \
    #             .filter(f.col('row') == 1).drop('row')
    #
    #         cases_filed_df = nclt_court.groupBy('company_master_id') \
    #             .agg(f.count('d_order_date').alias('number_of_cases_filed'))
    #
    #         cases_closed_df = nclt_court.groupBy('company_master_id') \
    #             .agg(f.count('disposed_date').alias('number_of_cases_closed'))
    #
    #         final_df = cases_filed_df.join(cases_closed_df, ['company_master_id'], 'outer')
    #         final_df = final_df \
    #             .withColumn('number_of_nclt_cases', f.col('number_of_cases_filed') - f.col('number_of_cases_closed')) \
    #             .select(f.col('company_master_id').astype('long'), f.current_date().alias('as_on_date'),
    #                     f.col('number_of_nclt_cases').astype('short'))
    #         return final_df
    #
    #
    # def get_criminal_case(high_court: DataFrame, dist_court: DataFrame, run_date: str = None):
    #     if run_date and datetime.datetime.strptime(run_date, '%Y-%m-%d').date() < datetime.datetime.now().date():
    #         high_disposed_date = f.to_date(f.col('crawl_dispose_date'), 'yyyy-MM-dd')
    #         high_court = high_court.withColumn('is_criminal', f.col('is_criminal').cast('integer')) \
    #             .withColumn('case_status', f.trim(f.lower(f.col('case_status')))) \
    #             .withColumn('d_decision_date',
    #                         f.when(high_disposed_date.isNotNull(), high_disposed_date).otherwise(f.current_date())) \
    #             .withColumnRenamed('case_no', 'case_number') \
    #             .withColumn('case_date', f.to_date(f.concat(f.col('case_year'), f.lit('-01-01')))) \
    #             .filter((f.col('case_date') <= f.current_date()) & (f.col('case_date') >= '1900-01-01') &
    #                     (f.col('d_decision_date') <= f.current_date()) & (f.col('d_decision_date') >= '1900-01-01') &
    #                     (f.col('case_date') <= f.col('d_decision_date')) &
    #                     (f.col('case_status').isin(['pending', 'case pending', 'disposed']))) \
    #             .select('company_master_id', 'case_number', 'd_decision_date', 'case_date', 'act', 'is_criminal')
    #
    #         window_high_unique_cases = Window.partitionBy('company_master_id', 'case_number')
    #         high_court = high_court.withColumn('case_date', f.min('case_date').over(window_high_unique_cases)) \
    #             .withColumn('d_decision_date', f.max('d_decision_date').over(window_high_unique_cases)) \
    #             .withColumn('is_criminal', f.max('is_criminal').over(window_high_unique_cases)) \
    #             .withColumn('row', f.row_number().over(window_high_unique_cases.orderBy(f.lit(None)))) \
    #             .filter(f.col('row') == 1).drop('row')
    #         ################################################################################################################
    #         dist_disposed_date = f.to_date(f.col('d_decision_date'), 'yyyy-MM-dd')
    #         dist_court = dist_court.withColumn('is_criminal', f.col('is_criminal').cast('integer')) \
    #             .withColumn('case_status', f.trim(f.lower(f.col('case_status')))) \
    #             .withColumn('d_decision_date',
    #                         f.when(dist_disposed_date.isNotNull(), dist_disposed_date).otherwise(f.current_date())) \
    #             .withColumn('case_date', f.to_date(f.concat(f.col('case_year'), f.lit('-01-01')))) \
    #             .filter((f.col('case_date') <= f.current_date()) & (f.col('case_date') >= '1900-01-01') &
    #                     (f.col('d_decision_date') <= f.current_date()) & (f.col('d_decision_date') >= '1900-01-01') &
    #                     (f.col('case_date') <= f.col('d_decision_date')) &
    #                     (f.col('case_status').isin(['pending', 'case pending', 'disposed']))) \
    #             .select('company_master_id', 'case_number', 'd_decision_date', 'case_date', 'act', 'is_criminal')
    #
    #         window_dist_unique_cases = Window.partitionBy('company_master_id', 'case_number')
    #         dist_court = dist_court.withColumn('case_date', f.min('case_date').over(window_dist_unique_cases)) \
    #             .withColumn('d_decision_date', f.max('d_decision_date').over(window_dist_unique_cases)) \
    #             .withColumn('is_criminal', f.max('is_criminal').over(window_dist_unique_cases)) \
    #             .withColumn('row', f.row_number().over(window_dist_unique_cases.orderBy(f.lit(None)))) \
    #             .filter(f.col('row') == 1).drop('row')
    #         ################################################################################################################
    #         dist_criminal_cases = dist_court.filter((f.col('is_criminal').cast('integer') == 1)) \
    #             .select('case_date', 'company_master_id', 'd_decision_date', 'case_number')
    #         high_criminal_cases = high_court.filter((f.col('is_criminal').cast('integer') == 1)) \
    #             .select('case_date', 'company_master_id', 'd_decision_date', 'case_number')
    #
    #         criminal_cases = dist_criminal_cases.union(high_criminal_cases)
    #         ################################################################################################################
    #         cases_filed_df = criminal_cases.groupBy('company_master_id', 'case_date') \
    #             .agg(f.count('case_number').alias('number_of_cases_filed')) \
    #             .withColumnRenamed('case_date', 'as_on_date')
    #         cases_closed_df = criminal_cases.groupBy('company_master_id', 'd_decision_date') \
    #             .agg(f.count('case_number').alias('number_of_cases_closed')) \
    #             .withColumnRenamed('d_decision_date', 'as_on_date')
    #
    #         final_df = cases_filed_df.join(cases_closed_df, ['company_master_id', 'as_on_date'], 'outer')
    #
    #         all_dates_df = criminal_cases.select('company_master_id').distinct().withColumn('start_date', start_date) \
    #             .withColumn('end_date', f.lit(f.current_date())) \
    #             .withColumn('as_on_date', f.explode(f.expr('sequence(start_date, end_date, interval 1 day)'))) \
    #             .select('company_master_id', 'as_on_date')
    #
    #         final_df = final_df.join(all_dates_df, ['company_master_id', 'as_on_date'], 'outer').fillna(
    #             {'number_of_cases_filed': 0, 'number_of_cases_closed': 0})
    #
    #         window = Window.partitionBy('company_master_id').orderBy('as_on_date').rowsBetween(Window.unboundedPreceding,
    #                                                                                            Window.currentRow)
    #
    #         final_df = final_df.withColumn('number_of_cases_filed', f.sum('number_of_cases_filed').over(window)) \
    #             .withColumn('number_of_cases_closed', f.sum('number_of_cases_closed').over(window))
    #
    #         final_df = final_df \
    #             .withColumn('no_of_criminal_cases', f.col('number_of_cases_filed') - f.col('number_of_cases_closed')) \
    #             .filter(f.col('as_on_date') == '2017-01-01').select(f.col('company_master_id').astype('long'), 'as_on_date',
    #                                                                 f.col('no_of_criminal_cases').astype('short'))
    #         return final_df
    #     else:
    #         high_disposed_date = f.to_date(f.col('crawl_dispose_date'), 'yyyy-MM-dd')
    #         high_court = high_court.withColumn('is_criminal', f.col('is_criminal').cast('integer')) \
    #             .withColumn('case_status', f.trim(f.lower(f.col('case_status')))) \
    #             .withColumn('d_decision_date', f.when(high_disposed_date.isNotNull(), high_disposed_date)) \
    #             .withColumnRenamed('case_no', 'case_number') \
    #             .withColumn('case_date', f.to_date(f.concat(f.col('case_year'), f.lit('-01-01')))) \
    #             .withColumn('valid_row', f.when(f.col('d_decision_date').isNull(), True)
    #                         .when((f.col('case_date') <= f.current_date()) & (f.col('case_date') >= '1900-01-01') &
    #                               (f.col('d_decision_date') <= f.current_date()) & (f.col('d_decision_date') >= '1900-01-01') &
    #                               (f.col('case_date') <= f.col('d_decision_date')), True).otherwise(False))\
    #             .filter(f.col('valid_row') & (f.col('case_status').isin(['pending', 'case pending', 'disposed']))) \
    #             .select('company_master_id', 'case_number', 'd_decision_date', 'case_date', 'act', 'is_criminal')
    #
    #         window_high_unique_cases = Window.partitionBy('company_master_id', 'case_number')
    #         high_court = high_court.withColumn('case_date', f.min('case_date').over(window_high_unique_cases)) \
    #             .withColumn('d_decision_date', f.max('d_decision_date').over(window_high_unique_cases)) \
    #             .withColumn('is_criminal', f.max('is_criminal').over(window_high_unique_cases)) \
    #             .withColumn('row', f.row_number().over(window_high_unique_cases.orderBy(f.lit(None)))) \
    #             .filter(f.col('row') == 1).drop('row')
    #         ################################################################################################################
    #         dist_disposed_date = f.to_date(f.col('d_decision_date'), 'yyyy-MM-dd')
    #         dist_court = dist_court.withColumn('is_criminal', f.col('is_criminal').cast('integer')) \
    #             .withColumn('case_status', f.trim(f.lower(f.col('case_status')))) \
    #             .withColumn('d_decision_date', f.when(dist_disposed_date.isNotNull(), dist_disposed_date))\
    #             .withColumn('case_date', f.to_date(f.concat(f.col('case_year'), f.lit('-01-01')))) \
    #             .withColumn('valid_row', f.when(f.col('d_decision_date').isNull(), True)
    #                         .when((f.col('case_date') <= f.current_date()) & (f.col('case_date') >= '1900-01-01') &
    #                               (f.col('d_decision_date') <= f.current_date()) & (f.col('d_decision_date') >= '1900-01-01') &
    #                               (f.col('case_date') <= f.col('d_decision_date')), True).otherwise(False))\
    #             .filter(f.col('valid_row') & (f.col('case_status').isin(['pending', 'case pending', 'disposed']))) \
    #             .select('company_master_id', 'case_number', 'd_decision_date', 'case_date', 'act', 'is_criminal')
    #
    #         window_dist_unique_cases = Window.partitionBy('company_master_id', 'case_number')
    #         dist_court = dist_court.withColumn('case_date', f.min('case_date').over(window_dist_unique_cases)) \
    #             .withColumn('d_decision_date', f.max('d_decision_date').over(window_dist_unique_cases)) \
    #             .withColumn('is_criminal', f.max('is_criminal').over(window_dist_unique_cases)) \
    #             .withColumn('row', f.row_number().over(window_dist_unique_cases.orderBy(f.lit(None)))) \
    #             .filter(f.col('row') == 1).drop('row')
    #         ################################################################################################################
    #         dist_criminal_cases = dist_court.filter((f.col('is_criminal').cast('integer') == 1)) \
    #             .select('case_date', 'company_master_id', 'd_decision_date', 'case_number')
    #         high_criminal_cases = high_court.filter((f.col('is_criminal').cast('integer') == 1)) \
    #             .select('case_date', 'company_master_id', 'd_decision_date', 'case_number')
    #
    #         criminal_cases = dist_criminal_cases.union(high_criminal_cases)
    #         ################################################################################################################
    #         cases_filed_df = criminal_cases.groupBy('company_master_id').agg(f.count('case_number')
    #                                                                          .alias('number_of_cases_filed'))
    #         cases_closed_df = criminal_cases.groupBy('company_master_id').agg(f.count('case_number')
    #                                                                           .alias('number_of_cases_closed'))
    #
    #         final_df = cases_filed_df.join(cases_closed_df, ['company_master_id'])
    #
    #         final_df = final_df \
    #             .withColumn('no_of_criminal_cases', f.col('number_of_cases_filed') - f.col('number_of_cases_closed')) \
    #             .select(f.col('company_master_id').astype('long'), f.current_date().alias('as_on_date'),
    #                     f.col('no_of_criminal_cases').astype('short'))
    #         return final_df
    #
    #
    # def get_check_bounce_cases(high_court: DataFrame, dist_court: DataFrame, run_date: str = None):
    #     if run_date and datetime.datetime.strptime(run_date, '%Y-%m-%d').date() < datetime.datetime.now().date():
    #         high_disposed_date = f.to_date(f.col('crawl_dispose_date'), 'yyyy-MM-dd')
    #         high_court = high_court.withColumn('is_criminal', f.col('is_criminal').cast('integer')) \
    #             .withColumn('case_status', f.trim(f.lower(f.col('case_status')))) \
    #             .withColumn('d_decision_date',
    #                         f.when(high_disposed_date.isNotNull(), high_disposed_date).otherwise(f.current_date())) \
    #             .withColumnRenamed('case_no', 'case_number') \
    #             .withColumn('case_date', f.to_date(f.concat(f.col('case_year'), f.lit('-01-01')))) \
    #             .filter((f.col('case_date') <= f.current_date()) & (f.col('case_date') >= '1900-01-01') &
    #                     (f.col('d_decision_date') <= f.current_date()) & (f.col('d_decision_date') >= '1900-01-01') &
    #                     (f.col('case_date') <= f.col('d_decision_date')) &
    #                     (f.col('case_status').isin(['pending', 'case pending', 'disposed']))) \
    #             .select('company_master_id', 'case_number', 'd_decision_date', 'case_date', 'act', 'is_criminal')
    #
    #         window_high_unique_cases = Window.partitionBy('company_master_id', 'case_number')
    #         high_court = high_court.withColumn('case_date', f.min('case_date').over(window_high_unique_cases)) \
    #             .withColumn('d_decision_date', f.max('d_decision_date').over(window_high_unique_cases)) \
    #             .withColumn('is_criminal', f.max('is_criminal').over(window_high_unique_cases)) \
    #             .withColumn('row', f.row_number().over(window_high_unique_cases.orderBy(f.lit(None)))) \
    #             .filter(f.col('row') == 1).drop('row')
    #         ################################################################################################################
    #         dist_disposed_date = f.to_date(f.col('d_decision_date'), 'yyyy-MM-dd')
    #         dist_court = dist_court.withColumn('is_criminal', f.col('is_criminal').cast('integer')) \
    #             .withColumn('case_status', f.trim(f.lower(f.col('case_status')))) \
    #             .withColumn('d_decision_date',
    #                         f.when(dist_disposed_date.isNotNull(), dist_disposed_date).otherwise(f.current_date())) \
    #             .withColumn('case_date', f.to_date(f.concat(f.col('case_year'), f.lit('-01-01')))) \
    #             .filter((f.col('case_date') <= f.current_date()) & (f.col('case_date') >= '1900-01-01') &
    #                     (f.col('d_decision_date') <= f.current_date()) & (f.col('d_decision_date') >= '1900-01-01') &
    #                     (f.col('case_date') <= f.col('d_decision_date')) &
    #                     (f.col('case_status').isin(['pending', 'case pending', 'disposed']))) \
    #             .select('company_master_id', 'case_number', 'd_decision_date', 'case_date', 'act', 'is_criminal')
    #
    #         window_dist_unique_cases = Window.partitionBy('company_master_id', 'case_number')
    #         dist_court = dist_court.withColumn('case_date', f.min('case_date').over(window_dist_unique_cases)) \
    #             .withColumn('d_decision_date', f.max('d_decision_date').over(window_dist_unique_cases)) \
    #             .withColumn('is_criminal', f.max('is_criminal').over(window_dist_unique_cases)) \
    #             .withColumn('row', f.row_number().over(window_dist_unique_cases.orderBy(f.lit(None)))) \
    #             .filter(f.col('row') == 1).drop('row')
    #         ################################################################################################################
    #         dist_criminal_cases = dist_court.filter(f.lower(f.trim(f.col('act'))).like("%negotiable instruments act%"))\
    #             .select('case_date', 'company_master_id', 'd_decision_date', 'case_number')
    #         high_criminal_cases = high_court.filter(f.lower(f.trim(f.col('act'))).like("%negotiable instruments act%"))\
    #             .select('case_date', 'company_master_id', 'd_decision_date', 'case_number')
    #
    #         criminal_cases = dist_criminal_cases.union(high_criminal_cases)
    #         ################################################################################################################
    #         cases_filed_df = criminal_cases.groupBy('company_master_id', 'case_date') \
    #             .agg(f.count('case_number').alias('number_of_cases_filed')) \
    #             .withColumnRenamed('case_date', 'as_on_date')
    #         cases_closed_df = criminal_cases.groupBy('company_master_id', 'd_decision_date') \
    #             .agg(f.count('case_number').alias('number_of_cases_closed')) \
    #             .withColumnRenamed('d_decision_date', 'as_on_date')
    #
    #         final_df = cases_filed_df.join(cases_closed_df, ['company_master_id', 'as_on_date'], 'outer')
    #
    #         all_dates_df = criminal_cases.select('company_master_id').distinct().withColumn('start_date', start_date) \
    #             .withColumn('end_date', f.lit(f.current_date())) \
    #             .withColumn('as_on_date', f.explode(f.expr('sequence(start_date, end_date, interval 1 day)'))) \
    #             .select('company_master_id', 'as_on_date')
    #
    #         final_df = final_df.join(all_dates_df, ['company_master_id', 'as_on_date'], 'outer').fillna(
    #             {'number_of_cases_filed': 0, 'number_of_cases_closed': 0})
    #
    #         window = Window.partitionBy('company_master_id').orderBy('as_on_date').rowsBetween(Window.unboundedPreceding,
    #                                                                                            Window.currentRow)
    #
    #         final_df = final_df.withColumn('number_of_cases_filed', f.sum('number_of_cases_filed').over(window)) \
    #             .withColumn('number_of_cases_closed', f.sum('number_of_cases_closed').over(window))
    #
    #         final_df = final_df \
    #             .withColumn('no_of_check_bounce_cases', f.col('number_of_cases_filed') - f.col('number_of_cases_closed')) \
    #             .filter(f.col('as_on_date') == '2017-01-01').select(f.col('company_master_id').astype('long'), 'as_on_date',
    #                                                                 f.col('no_of_check_bounce_cases').astype('short'))
    #         return final_df
    #     else:
    #         high_disposed_date = f.to_date(f.col('crawl_dispose_date'), 'yyyy-MM-dd')
    #         high_court = high_court.withColumn('is_criminal', f.col('is_criminal').cast('integer')) \
    #             .withColumn('case_status', f.trim(f.lower(f.col('case_status')))) \
    #             .withColumn('d_decision_date', f.when(high_disposed_date.isNotNull(), high_disposed_date)) \
    #             .withColumnRenamed('case_no', 'case_number') \
    #             .withColumn('case_date', f.to_date(f.concat(f.col('case_year'), f.lit('-01-01')))) \
    #             .withColumn('valid_row', f.when(f.col('d_decision_date').isNull(), True)
    #                         .when((f.col('case_date') <= f.current_date()) & (f.col('case_date') >= '1900-01-01') &
    #                               (f.col('d_decision_date') <= f.current_date()) & (f.col('d_decision_date') >= '1900-01-01') &
    #                               (f.col('case_date') <= f.col('d_decision_date')), True).otherwise(False))\
    #             .filter(f.col('valid_row') & (f.col('case_status').isin(['pending', 'case pending', 'disposed']))) \
    #             .select('company_master_id', 'case_number', 'd_decision_date', 'case_date', 'act', 'is_criminal')
    #
    #         window_high_unique_cases = Window.partitionBy('company_master_id', 'case_number')
    #         high_court = high_court.withColumn('case_date', f.min('case_date').over(window_high_unique_cases)) \
    #             .withColumn('d_decision_date', f.max('d_decision_date').over(window_high_unique_cases)) \
    #             .withColumn('is_criminal', f.max('is_criminal').over(window_high_unique_cases)) \
    #             .withColumn('row', f.row_number().over(window_high_unique_cases.orderBy(f.lit(None)))) \
    #             .filter(f.col('row') == 1).drop('row')
    #         ################################################################################################################
    #         dist_disposed_date = f.to_date(f.col('d_decision_date'), 'yyyy-MM-dd')
    #         dist_court = dist_court.withColumn('is_criminal', f.col('is_criminal').cast('integer')) \
    #             .withColumn('case_status', f.trim(f.lower(f.col('case_status')))) \
    #             .withColumn('d_decision_date', f.when(dist_disposed_date.isNotNull(), dist_disposed_date))\
    #             .withColumn('case_date', f.to_date(f.concat(f.col('case_year'), f.lit('-01-01')))) \
    #             .withColumn('valid_row', f.when(f.col('d_decision_date').isNull(), True)
    #                         .when((f.col('case_date') <= f.current_date()) & (f.col('case_date') >= '1900-01-01') &
    #                               (f.col('d_decision_date') <= f.current_date()) & (f.col('d_decision_date') >= '1900-01-01') &
    #                               (f.col('case_date') <= f.col('d_decision_date')), True).otherwise(False))\
    #             .filter(f.col('valid_row') & (f.col('case_status').isin(['pending', 'case pending', 'disposed']))) \
    #             .select('company_master_id', 'case_number', 'd_decision_date', 'case_date', 'act', 'is_criminal')
    #
    #         window_dist_unique_cases = Window.partitionBy('company_master_id', 'case_number')
    #         dist_court = dist_court.withColumn('case_date', f.min('case_date').over(window_dist_unique_cases)) \
    #             .withColumn('d_decision_date', f.max('d_decision_date').over(window_dist_unique_cases)) \
    #             .withColumn('is_criminal', f.max('is_criminal').over(window_dist_unique_cases)) \
    #             .withColumn('row', f.row_number().over(window_dist_unique_cases.orderBy(f.lit(None)))) \
    #             .filter(f.col('row') == 1).drop('row')
    #         ################################################################################################################
    #         dist_criminal_cases = dist_court.filter(f.lower(f.trim(f.col('act'))).like("%negotiable instruments act%"))\
    #             .select('case_date', 'company_master_id', 'd_decision_date', 'case_number')
    #         high_criminal_cases = high_court.filter(f.lower(f.trim(f.col('act'))).like("%negotiable instruments act%"))\
    #             .select('case_date', 'company_master_id', 'd_decision_date', 'case_number')
    #
    #         criminal_cases = dist_criminal_cases.union(high_criminal_cases)
    #         ################################################################################################################
    #         cases_filed_df = criminal_cases.groupBy('company_master_id').agg(f.count('case_number')
    #                                                                          .alias('number_of_cases_filed'))
    #         cases_closed_df = criminal_cases.groupBy('company_master_id').agg(f.count('case_number')
    #                                                                           .alias('number_of_cases_closed'))
    #
    #         final_df = cases_filed_df.join(cases_closed_df, ['company_master_id'])
    #
    #         final_df = final_df \
    #             .withColumn('no_of_check_bounce_cases', f.col('number_of_cases_filed') - f.col('number_of_cases_closed')) \
    #             .select(f.col('company_master_id').astype('long'), f.current_date().alias('as_on_date'),
    #                     f.col('no_of_check_bounce_cases').astype('short'))
    #         return final_df

    ####################################################################################################################

    def get_litigation_features(self):
        live_crawler_output_connection = utils.get_db_connection('live_crawler_output')

        # sat_litigation_data_query = generate_query(
        #     table_name='tmp.yml_sat_litigation_data',
        #     select_columns=['company_master_id', 'd_date', 'link'],
        #     # filter_columns={'company_master_id': 'int'},
        #     # filter_values={'company_master_id': self.companies},
        #     limit=1000
        # )
        #
        # sat_litigation_data = pd.read_sql(sat_litigation_data_query, live_crawler_output_connection)

        bifr_litigation_data_query = generate_query(
            table_name='tmp.yml_bifr_litigation_data',
            select_columns=['company_master_id', 'case_year', 'status', 'd_date_of_last_order',
                            'case_number'],
            # filter_columns={'company_master_id': 'int', },
            # filter_values={'company_master_id': self.companies},
            limit=100
        )

        bifr_litigation_data = pd.read_sql(bifr_litigation_data_query, live_crawler_output_connection)
        #
        # print(bifr_litigation_data.to_string())

        # nclt_litigation_data_query = generate_query(
        #     table_name='tmp.yml_nclt_litigation_data',
        #     select_columns=['*'],
        #     filter_columns={'company_master_id': 'int', 'financial_year': 'str'},
        #     filter_values={'company_master_id': self.companies, 'financial_year': [self.as_on_dt_fy]}
        # )
        #
        # nclt_litigation_data = pd.read_sql(nclt_litigation_data_query, live_crawler_output_connection)
        #
        # high_court_litigation_data_query = generate_query(
        #     table_name='tmp.yml_high_court_litigation_data',
        #     select_columns=['*'],
        #     filter_columns={'company_master_id': 'int', 'financial_year': 'str'},
        #     filter_values={'company_master_id': self.companies, 'financial_year': [self.as_on_dt_fy]}
        # )
        #
        # high_court_litigation_data = pd.read_sql(high_court_litigation_data_query, live_crawler_output_connection)
        #
        # district_court_litigation_data_query = generate_query(
        #     table_name='tmp.yml_district_court_litigation_data',
        #     select_columns=['*'],
        #     filter_columns={'company_master_id': 'int', 'financial_year': 'str'},
        #     filter_values={'company_master_id': self.companies, 'financial_year': [self.as_on_dt_fy]}
        # )
        #
        # district_court_litigation_data = pd.read_sql(district_court_litigation_data_query, live_crawler_output_connection)

        # sat_feat = self.sat_cases(sat_litigation_data)
        bifr_feat = self.bifr_cases(bifr_litigation_data)
        # nclt_feat = self.nclt_cases(nclt_litigation_data)
        # criminal_case_feat = self.get_criminal_case(high_court_litigation_data, district_court_litigation_data)
        # check_bounce_feat = self.get_check_bounce_cases(high_court_litigation_data, district_court_litigation_data)

        # feat = bifr_feat.join(sat_feat, on=['company_master_id', 'as_on_date'], how='outer')
        # feat = feat.join(nclt_feat, on=['company_master_id', 'as_on_date'], how='outer')
        # feat = feat.join(criminal_case_feat, on=['company_master_id', 'as_on_date'], how='outer')
        # feat = feat.join(check_bounce_feat, on=['company_master_id', 'as_on_date'], how='outer')

        # return sat_feat
