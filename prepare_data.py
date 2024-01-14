import pandas as pd
import automation_configs
import numpy as np
import mysql.connector
from contextlib import contextmanager

@contextmanager
def connect(conn_string):
    conn = mysql.connector.connect(
        user=conn_string.user,
        password=conn_string.password,
        host=conn_string.host,
        database=conn_string.database,
    )
    try:
        yield conn
    finally:
        conn.close()


def get_raw_data(conn_string, query):
    with connect(conn_string) as conn:
        df_from_query = pd.read_sql(query, conn)
    return df_from_query




def query_for_raw_data(stores_for_deployment):
    return f"""
with pred_and_actual as (
SELECT
StoreNo,
DueDate,
CAST(TIMESTAMPDIFF(SECOND, oh.ordertime, oh.EvaluatedDeliveryTime)
/ 60.00 AS DECIMAL(10,2)) as actual_dlv_time
,PromiseSeconds/60 as PromiseMin

FROM
    OrdersHistory as oh
WHERE
    SaleType = 2 and StoreNo in {tuple(stores_for_deployment)}
    and IsTimedOrder = 0

)
Select * from pred_and_actual

"""
def query_for_test(date): 
    return f"""
    SELECT
    StoreNo,
    DueDate,
    OrderId
    FROM
        OrdersHistory
    WHERE DueDate = '{date}'

"""

def query_for_test_2(): 
    return f"""
    SELECT
    StoreNo,
    DueDate,
    FROM
        OrdersHistory

"""

def query_for_test_3(store_numbers, start_date, end_date): 
    return f"""
    SELECT
    StoreNo,
    DueDate
    FROM
        OrdersHistory
    WHERE
        StoreNo IN ({', '.join(map(str, store_numbers))})
        AND DueDate BETWEEN '{start_date}' AND '{end_date}'
"""

def query_for_monitoring_2(store_numbers, start_date, end_date): 
    return f"""
   SELECT
        DATE_FORMAT(MIN(DueDate), '%b %D')AS 'START DATE',
        DATE_FORMAT(MAX(DueDate), '%b %D')AS 'END DATE',
        count(distinct duedate)as NumOfDays,
        StoreNo,
        COUNT( * )AS '# Of Deliveries',
        CONCAT(ROUND(CAST(COUNT(CASE WHEN AggCarrierId IS NOT NULL
                        AND AggCreatedAt IS NOT NULL
                        AND AggOrderID IS NOT NULL
                        THEN 1 ELSE NULL END)AS FLOAT) / COUNT( * ) * 100, 2), '%')AS '% of Agg Orders',
        -- ROUND(AVG(CAST(TIMESTAMPDIFF(MINUTE, OrderTime, EvaluatedDeliveryTime)AS FLOAT)), 2)AS 'Avg Delivery Time',
        CONCAT(ROUND(CAST(COUNT(CASE WHEN TIMESTAMPDIFF(SECOND, DATE_ADD(OrderTime, INTERVAL PromiseSeconds SECOND), EvaluatedDeliveryTime) <= -600 THEN 1 END)AS FLOAT) / COUNT( * ) * 100, 2), '%')AS 'Early by over10',
        CONCAT(ROUND(CAST(COUNT(CASE WHEN TIMESTAMPDIFF(SECOND, DATE_ADD(OrderTime, INTERVAL PromiseSeconds SECOND), EvaluatedDeliveryTime) >= 600 THEN 1 END)AS FLOAT) / COUNT( * ) * 100, 2), '%')AS 'Late by over10'
                            FROM
                            OrdersHistory
                            WHERE
                            SaleType = 2 AND OrderStatus = 6
                                /*AFTER */
                                AND DueDate BETWEEN '{start_date}' AND '{end_date}'
                                AND StoreNo IN ({', '.join(map(str, store_numbers))})
                                GROUP BY
                                StoreNo
                                ORDER BY
                                StoreNo;

"""




def get_raw_data_w_errors():
    query_raw_data = query_for_raw_data(automation_configs.stores_for_deployment)
    raw_data = get_raw_data(automation_configs.algo_conn_details, query_raw_data)
    raw_data_with_errors = raw_data.assign(
        error=lambda df: df["actual_dlv_time"] - df["PromiseMin"]
    ).assign(
        error_larger_than_10=lambda df: np.where(df.error >= 10, 1, 0),
        error_smaller_than_10=lambda df: np.where(df.error <= -10, 1, 0),
    )
    return raw_data_with_errors


def get_performace_per_store(raw_data_with_errors, start_date, end_date):
    performace_per_store = (
        raw_data_with_errors.query(
            f'DueDate>="{start_date}" and DueDate <="{end_date}"'
        )
        .dropna()
        .groupby("StoreNo")
        .agg(
            pct_late=("error_larger_than_10", np.mean),
            pct_early=("error_smaller_than_10", np.mean),
            orders_count=("error_larger_than_10", "count"),
        )
    )
    return performace_per_store


def get_performace_for_market(raw_data_with_errors, start_date, end_date):
    agg_results_for_makret = (
        raw_data_with_errors.query(
            f'DueDate>="{start_date}" and DueDate <="{end_date}"'
        )
        .dropna()[["error_larger_than_10", "error_smaller_than_10"]]
        .rename(
            columns={
                "error_larger_than_10": "pct_late",
                "error_smaller_than_10": "pct_early",
            }
        )
        .mean()
        .rename("metric_results")
        .to_frame()
        .T
    )
    return agg_results_for_makret


class DataForAnalysis:
    def __init__(self, baseline_per_store, model_per_store, agg_baseline, agg_model):
        self.baseline_per_store = baseline_per_store
        self.model_per_store = model_per_store
        self.agg_baseline = agg_baseline
        self.agg_model = agg_model
        self.raw_data_with_errors = None


def get_ref_market_per_store(raw_data_with_errors):
    performace_baseline_per_store = get_performace_per_store(
        raw_data_with_errors,
        automation_configs.start_date_baseline,
        automation_configs.end_date_baseline,
    )
    performace_model_per_store = get_performace_per_store(
        raw_data_with_errors, automation_configs.start_date_pilot, automation_configs.end_date_pilot
    )
    agg_results_baseline = get_performace_for_market(
        raw_data_with_errors,
        automation_configs.start_date_baseline,
        automation_configs.end_date_baseline,
    )

    agg_results_model = get_performace_for_market(
        raw_data_with_errors, automation_configs.start_date_pilot, automation_configs.end_date_pilot
    )
    data_for_anaysis_class = DataForAnalysis(
        performace_baseline_per_store,
        performace_model_per_store,
        agg_results_baseline,
        agg_results_model,
    )
    return data_for_anaysis_class


def get_data_for_analysis():
    raw_data_with_errors = get_raw_data_w_errors()
    data_for_anaysis_class = get_ref_market_per_store(raw_data_with_errors)
    data_for_anaysis_class.raw_data_with_errors = raw_data_with_errors
    return data_for_anaysis_class
