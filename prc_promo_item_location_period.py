from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.types import LongType, IntegerType
import pyspark.sql.functions as F
import os
import logging
from jdbs_db_utils import write_jdbc_data,read_jdbc_data
from spark_utils import create_spark_session

logger = logging.getLogger(__name__)

#working tables
result_promo_item_location_period='result_promo_item_location_period'


#working tables
property_norm = "property_norm"
promo_item_location_period_norm = "promo_item_location_period_norm"
promo_item_location_period = "promo_item_location_period"
result_supplier_location_period = "result_supplier_location_period"


condition = "condition"
unit = "unit"

def create_root_calc_id(spark: SparkSession, table_name):
    root_calc = read_jdbc_data(spark=spark, table_name=table_name)
    max_root_calc_id = root_calc.agg(F.max("reversal_id")).collect()[0][0] + 1
    return max_root_calc_id

def extract_data_for_procedure(spark: SparkSession):
    """Extracts data from necessary SQL tables."""

    dfs = {
        "property_norm": read_jdbc_data(property_norm, spark),
        "promo_item_location_period_norm": read_jdbc_data(promo_item_location_period_norm, spark),
        "promo_item_location_period": read_jdbc_data(promo_item_location_period, spark),
        "condition": read_jdbc_data(condition, spark),
        "unit": read_jdbc_data(unit, spark)
    }

    return dfs


def find_intersections(
        promo_item_location_period_norm_df: DataFrame,
        property_norm_df: DataFrame,
        metric_id: int
) -> DataFrame:
    """
    Finds all valid intersections between scope data и properties.
    This function is PySpark-version of CTE 'preres' from procedure prc_reversal_promo_item_location_period.
    """

    scope = promo_item_location_period_norm_df.alias("scope")
    prop = property_norm_df.alias("prop")

    join_condition = (
            (F.col("prop.metric_id") == metric_id) &
            (F.col("prop.start_date") <= F.col("scope.end_dt")) &
            (F.col("prop.end_date") >= F.col("scope.begin_dt"))
    )

    cluster_cols_regular = [
        "item_category_level_0_uid", "item_category_level_1_uid",
        "item_category_level_2_uid", "item_category_level_3_uid",
        "item_category_level_4_uid", "item_expiration_period_uid",
        "promo_period_uid", "promo_category_level_0_uid",
        "promo_category_level_1_uid", "promo_category_level_2_uid"
    ]

    cluster_cols_with_zero = [
        "store_format_level_0_uid", "store_format_level_1_uid",
        "delivery_group_uid"
    ]

    for col_name in cluster_cols_regular:
        join_condition = join_condition & (
                F.coalesce(F.col(f"prop.{col_name}"), F.col(f"scope.{col_name}")) == F.col(f"scope.{col_name}")
        )

    for col_name in cluster_cols_with_zero:
        join_condition = join_condition & (
                F.coalesce(F.col(f"prop.{col_name}"), F.col(f"scope.{col_name}"), F.lit(0)) == \
                F.coalesce(F.col(f"scope.{col_name}"), F.lit(0))
        )

    intersections_df = scope.join(
        prop,
        join_condition,
        "inner"
    ).select(
        F.col("scope.item_id"),
        F.col("scope.location_id"),
        F.col("scope.promo_id"),
        F.greatest(F.col("prop.start_date"), F.col("scope.begin_dt")).alias("begin_dt"),
        F.least(F.col("prop.end_date"), F.col("scope.end_dt")).alias("end_dt"),
        F.col("prop.property_id"),
        F.col("prop.is_exception"),
        F.col("prop.order_number")
    )

    return intersections_df


def calculate_final_periods(intersections_df: DataFrame):
    """
    Does date period slicing based on priorities.
    This function is PySpark-version of SQL function fnc_get_period and
    window functions in SQL procedure prc_promo_item_location_period.
    """

    scope_pk_cols = ["item_id", "location_id", "promo_id"]

    event_points_df = intersections_df.select(
        *scope_pk_cols,
        F.array(
            F.col("begin_dt"),
            F.col("end_dt") + F.expr("INTERVAL 1 DAY")
        ).alias("dates")
    ).groupBy(*scope_pk_cols).agg(
        F.array_sort(F.flatten(F.collect_set("dates"))).alias("event_dates")
    )

    intervals_df = event_points_df.select(
        *scope_pk_cols,
        F.explode("event_dates").alias("interval_start")
    ).withColumn(
        "interval_end",
        (F.lead("interval_start", 1).over(Window.partitionBy(*scope_pk_cols).orderBy("interval_start")) - F.expr(
            "INTERVAL 1 DAY")).cast("date")
    ).dropna(subset=["interval_end"])

    interval_props_df = intervals_df.join(
        intersections_df,
        scope_pk_cols
    ).filter(
        (F.col("interval_start") <= F.col("end_dt")) &
        (F.col("interval_end") >= F.col("begin_dt"))
    )

    win_spec = Window.partitionBy(*scope_pk_cols, "interval_start") \
        .orderBy(F.col("is_exception").desc(), F.col("order_number").asc())

    best_prop_df = interval_props_df.withColumn("rn", F.row_number().over(win_spec)).filter(F.col("rn") == 1)

    change_detector_win = Window.partitionBy(*scope_pk_cols).orderBy("interval_start")
    coalesce_df = best_prop_df.withColumn("prev_prop", F.lag("property_id", 1).over(change_detector_win)) \
        .withColumn("is_new_group", F.when(F.col("property_id") != F.col("prev_prop"), 1).otherwise(0)) \
        .withColumn("group_id", F.sum("is_new_group").over(change_detector_win))

    final_periods_df = coalesce_df.groupBy(*scope_pk_cols, "group_id", "property_id", "is_exception", "order_number") \
        .agg(
        F.min("interval_start").alias("begin_dt"),
        F.max("interval_end").alias("end_dt")
    ).select(
        *scope_pk_cols,
        "begin_dt",
        "end_dt",
        "property_id",
        "is_exception",
        "order_number"
    ).orderBy(*scope_pk_cols, "begin_dt")

    return final_periods_df


def calculate_gaps(final_periods_df: DataFrame, original_scope_df: DataFrame):
    """
    Calculates "gaps" - date periods without any property.
    This function is PySpark-version of the 2nd INSERT-block from SQL procedure prc_reversal_promo_item_location_period.
    """
    scope_pk_cols = ["item_id", "location_id", "promo_id"]

    final_periods_aliased = final_periods_df.alias("final")
    original_scope_aliased = original_scope_df.alias("orig")

    lag_window = Window.partitionBy(*scope_pk_cols).orderBy(F.col("final.begin_dt"))

    processed_periods = final_periods_aliased.withColumn("prev_end_dt", F.lag("final.end_dt", 1).over(lag_window)) \
        .join(original_scope_aliased, scope_pk_cols) \
        .withColumn("min_begin_dt", F.min("final.begin_dt").over(Window.partitionBy(*scope_pk_cols))) \
        .withColumn("max_end_dt", F.max("final.end_dt").over(Window.partitionBy(*scope_pk_cols)))

    gap_at_start = processed_periods \
        .filter(F.col("final.begin_dt") == F.col("min_begin_dt")) \
        .filter(F.col("orig.begin_dt") < F.col("final.begin_dt")) \
        .select(
        *scope_pk_cols,
        F.col("orig.begin_dt").alias("begin_dt"),
        (F.col("final.begin_dt") - F.expr("INTERVAL 1 DAY")).alias("end_dt")
    )

    gap_at_end = processed_periods \
        .filter(F.col("final.end_dt") == F.col("max_end_dt")) \
        .filter(F.col("orig.end_dt") > F.col("final.end_dt")) \
        .select(
        *scope_pk_cols,
        (F.col("final.end_dt") + F.expr("INTERVAL 1 DAY")).alias("begin_dt"),
        F.col("orig.end_dt").alias("end_dt")
    )

    gaps_in_middle = processed_periods \
        .filter(F.col("prev_end_dt").isNotNull()) \
        .filter(F.col("final.begin_dt") - F.expr("INTERVAL 1 DAY") > F.col("prev_end_dt")) \
        .select(
        *scope_pk_cols,
        (F.col("prev_end_dt") + F.expr("INTERVAL 1 DAY")).alias("begin_dt"),
        (F.col("final.begin_dt") - F.expr("INTERVAL 1 DAY")).alias("end_dt")
    )

    gaps_df = gap_at_start.unionByName(gap_at_end).unionByName(gaps_in_middle)

    return gaps_df


def find_unmatched_scopes(final_periods_df: DataFrame, original_scope_df: DataFrame):
    """
    Finds rows in the scope without any property.
    This function is PySpark-version of the 3rd INSERT-block from SQL procedure prc_reversal_promo_item_location_period.
    """
    scope_pk_cols = ["item_id", "location_id", "promo_id"]

    matched_scopes = final_periods_df.select(*scope_pk_cols).distinct()

    unmatched_df = original_scope_df.join(
        matched_scopes,
        scope_pk_cols,
        "left_anti"
    )

    return unmatched_df


def assemble_final_result(
        final_periods_df: DataFrame,
        gaps_df: DataFrame,
        unmatched_df: DataFrame,
        condition_df: DataFrame,
        unit_df: DataFrame,
        metric_id: int,
        p_root_calc_id: int
):
    """
    Assembles all previous scope parts into result dataframe, joining condition and unit.
    THis function is PySpark-version of the final SELECT-block from SQL procedure prc_reversal_promo_item_location_period.
    """
    scope_pk_cols = ["item_id", "location_id", "promo_id"]

    periods_with_props = final_periods_df.withColumn("metric_id", F.lit(metric_id))

    gaps_prepared_df = gaps_df.withColumn("property_id", F.lit(None).cast(LongType())) \
        .withColumn("is_exception", F.lit(False)) \
        .withColumn("order_number", F.lit(None).cast(IntegerType())) \
        .withColumn("metric_id", F.lit(metric_id))

    unmatched_prepared_df = unmatched_df.withColumn("property_id", F.lit(None).cast(LongType())) \
        .withColumn("is_exception", F.lit(False)) \
        .withColumn("order_number", F.lit(None).cast(IntegerType())) \
        .withColumn("metric_id", F.lit(metric_id))

    combined_df = periods_with_props.unionByName(gaps_prepared_df).unionByName(unmatched_prepared_df)

    final_df = combined_df.join(
        condition_df,
        "property_id",
        "left"
    ).join(
        unit_df,
        F.col("unit_id") == unit_df.id,
        "left"
    ).select(
        *scope_pk_cols,
        F.col("begin_dt"),
        F.col("end_dt"),
        F.lit(p_root_calc_id).alias("root_calc_id"),
        F.col("metric_id").cast(LongType()),
        F.col("unit_id"),
        F.col("operator"),
        F.col("value"),
        F.col("value_data_type"),
        F.col("is_exception")
    ).orderBy(*scope_pk_cols, "begin_dt")

    return final_df


def main(current_metric_id):
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("INFO: Step 1: Spark session created.")

    p_root_calc_id = create_root_calc_id(spark, "reversal")
    print("INFO: Step 2: Root calculation id created.")

    dfs = extract_data_for_procedure(spark)

    intersections_df = find_intersections(
        promo_item_location_period_norm_df=dfs["promo_item_location_period_norm"],
        property_norm_df=dfs["property_norm"],
        metric_id=current_metric_id  # Passing metric_id for filtering
    )

    print("INFO: Step 3 results: Found intersections")

    final_periods_df = calculate_final_periods(intersections_df)
    print("INFO: Step 4 results: Final period after slicing and gluing")

    gaps_df = calculate_gaps(final_periods_df, dfs["promo_item_location_period"])
    print("INFO: Step 5 results: Found 'gaps' (periods without property)")

    unmatched_df = find_unmatched_scopes(final_periods_df, dfs["promo_item_location_period"])
    print("INFO: Step 6 results: Unmatched scopes")

    final_result_df = assemble_final_result(
        final_periods_df=final_periods_df,
        gaps_df=gaps_df,
        unmatched_df=unmatched_df,
        condition_df=dfs["condition"],
        unit_df=dfs["unit"],
        metric_id=current_metric_id,
        p_root_calc_id=p_root_calc_id
    )

    print("INFO: Step 7: Writing final result_promo_item_location_period to PostgreSQL...")
    write_jdbc_data(final_result_df, result_promo_item_location_period)
    print("INFO: Process completed successfully.")

    spark.stop()


metric_id = os.environ.get("METRIC_ID")
main(metric_id)