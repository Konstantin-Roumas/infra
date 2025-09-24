from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.types import LongType, IntegerType, DateType, ArrayType, StructType, StructField
import pyspark.sql.functions as F
import os
import logging
from jdbs_db_utils import write_jdbc_data,read_jdbc_data
from spark_utils import create_spark_session
from datetime import timedelta

logger = logging.getLogger(__name__)


property_norm = "property_norm"
item_location_period_norm = "item_location_period_norm"
item_location_period = "item_location_period"
condition = "condition"
unit = "unit"

#working tables
result_item_location_period='result_item_location_period'



def create_root_calc_id(spark: SparkSession, table_name):
    root_calc = read_jdbc_data(spark=spark, table_name=table_name)
    max_root_calc_id = root_calc.agg(F.max("reversal_id")).collect()[0][0] + 1
    return max_root_calc_id

def extract_data_for_procedure(spark: SparkSession):
    """Extracts data from necessary SQL tables."""
    dfs = {
        "property_norm": read_jdbc_data(property_norm, spark),
        "item_location_period_norm": read_jdbc_data(item_location_period_norm, spark),
        "item_location_period": read_jdbc_data(item_location_period, spark),
        "condition": read_jdbc_data(condition, spark),
        "unit": read_jdbc_data(unit, spark)
    }
    return dfs


def find_intersections(
        item_location_period_norm_df: DataFrame,
        property_norm_df: DataFrame,
        metric_id: int
) -> DataFrame:
    """Finds all valid intersections between scope data and properties."""
    scope = item_location_period_norm_df.alias("scope")
    prop = property_norm_df.alias("prop")

    join_condition = (
        (F.col("prop.metric_id") == metric_id) &
        (F.col("prop.start_date") < F.col("scope.end_dt") + F.expr("INTERVAL 1 DAY")) &
        (F.col("scope.begin_dt") < F.col("prop.end_date") + F.expr("INTERVAL 1 DAY"))
    )

    cluster_cols_regular = [
        "item_category_level_0_uid", "item_category_level_1_uid",
        "item_category_level_2_uid", "item_category_level_3_uid",
        "item_category_level_4_uid", "item_expiration_period_uid"
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
            F.coalesce(F.col(f"prop.{col_name}"), F.col(f"scope.{col_name}"), F.lit(0)) ==
            F.coalesce(F.col(f"scope.{col_name}"), F.lit(0))
        )

    intersections_df = scope.join(prop, join_condition, "inner").select(
        F.col("scope.item_id"),
        F.col("scope.location_id"),
        F.greatest(F.col("prop.start_date"), F.col("scope.begin_dt")).alias("begin_dt"),
        F.least(F.col("prop.end_date"), F.col("scope.end_dt")).alias("end_dt"),
        F.col("prop.property_id"),
        F.col("prop.is_exception"),
        F.col("prop.order_number")
    )
    return intersections_df

# ======================================================================================
# REFACTORED SECTION: The calculate_final_periods function has been completely replaced.
# ======================================================================================

def calculate_final_periods(intersections_df: DataFrame) -> DataFrame:
    """
    Calculates final periods using the "Iterative Subtraction" algorithm to match the
    PostgreSQL fnc_get_period logic. It subtracts date ranges of higher-priority
    properties from lower-priority ones.
    """
    scope_pk_cols = ["item_id", "location_id"]

    # Define a UDF to perform the period subtraction for a single row.
    def subtract_periods_udf_logic(current_start, current_end, blocker_starts, blocker_ends):
        if current_start > current_end:
            return []
            
        if blocker_starts is None or len(blocker_starts) == 0:
            return [(current_start, current_end)]

        blockers = sorted(zip(blocker_starts, blocker_ends))
        
        # Initial period to be chipped away
        available_periods = [(current_start, current_end)]

        for b_start, b_end in blockers:
            if b_start > b_end: # Skip invalid blocker periods
                continue

            next_available_periods = []
            for a_start, a_end in available_periods:
                # No overlap: blocker is before or after the available period
                if b_end < a_start or b_start > a_end:
                    next_available_periods.append((a_start, a_end))
                    continue

                # Left part remains
                if a_start < b_start:
                    new_end = b_start - timedelta(days=1)
                    if a_start <= new_end: # Defensive check
                        next_available_periods.append((a_start, new_end))
                
                # Right part remains
                if a_end > b_end:
                    new_start = b_end + timedelta(days=1)
                    if new_start <= a_end: # Defensive check
                        next_available_periods.append((new_start, a_end))
            
            available_periods = next_available_periods
            if not available_periods:
                break # Nothing left to subtract from
        
        return available_periods

    # Define the schema for the UDF's return type
    udf_return_schema = ArrayType(StructType([
        StructField("start_dt", DateType(), False),
        StructField("end_dt", DateType(), False)
    ]))

    subtract_periods_udf = F.udf(subtract_periods_udf_logic, udf_return_schema)

    # Define a window to get all preceding (higher-priority) date ranges
    priority_window = Window.partitionBy(*scope_pk_cols) \
        .orderBy(F.col("is_exception").desc(), F.col("order_number").asc(), F.col("property_id").asc()) \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow-1)

    # For each property, collect the date ranges of all higher-priority properties
    periods_with_blockers = intersections_df.withColumn(
        "blocker_starts", F.collect_list("begin_dt").over(priority_window)
    ).withColumn(
        "blocker_ends", F.collect_list("end_dt").over(priority_window)
    )

    # Apply the UDF to calculate the final, valid periods for each property
    result_with_period_arrays = periods_with_blockers.withColumn(
        "final_periods",
        subtract_periods_udf(F.col("begin_dt"), F.col("end_dt"), F.col("blocker_starts"), F.col("blocker_ends"))
    )

    # Explode the array of structs into separate rows and filter out empty results
    final_periods_df = result_with_period_arrays \
        .filter(F.size("final_periods") > 0) \
        .withColumn("period", F.explode("final_periods")) \
        .select(
            *scope_pk_cols,
            F.col("period.start_dt").alias("begin_dt"),
            F.col("period.end_dt").alias("end_dt"),
            "property_id",
            "is_exception",
            "order_number"
        ).orderBy(*scope_pk_cols, "begin_dt")

    return final_periods_df


def calculate_gaps(final_periods_df: DataFrame, original_scope_df: DataFrame) -> DataFrame:
    """Calculates "gaps" - date periods without any property."""
    scope_pk_cols = ["item_id", "location_id"]
    final_periods_aliased = final_periods_df.alias("final")
    original_scope_aliased = original_scope_df.alias("orig")

    lag_window = Window.partitionBy(*scope_pk_cols).orderBy(F.col("final.begin_dt"))

    processed_periods = final_periods_aliased.withColumn("prev_end_dt", F.lag("final.end_dt", 1).over(lag_window)) \
        .join(original_scope_aliased, scope_pk_cols) \
        .withColumn("min_begin_dt", F.min("final.begin_dt").over(Window.partitionBy(*scope_pk_cols))) \
        .withColumn("max_end_dt", F.max("final.end_dt").over(Window.partitionBy(*scope_pk_cols)))

    gap_at_start = processed_periods \
        .filter((F.col("final.begin_dt") == F.col("min_begin_dt")) & (F.col("orig.begin_dt") < F.col("final.begin_dt"))) \
        .select(*scope_pk_cols, F.col("orig.begin_dt").alias("begin_dt"), (F.col("final.begin_dt") - F.expr("INTERVAL 1 DAY")).alias("end_dt"))

    gap_at_end = processed_periods \
        .filter((F.col("final.end_dt") == F.col("max_end_dt")) & (F.col("orig.end_dt") > F.col("final.end_dt"))) \
        .select(*scope_pk_cols, (F.col("final.end_dt") + F.expr("INTERVAL 1 DAY")).alias("begin_dt"), F.col("orig.end_dt").alias("end_dt"))

    gaps_in_middle = processed_periods \
        .filter(F.col("prev_end_dt").isNotNull() & (F.col("final.begin_dt") - F.expr("INTERVAL 1 DAY") > F.col("prev_end_dt"))) \
        .select(*scope_pk_cols, (F.col("prev_end_dt") + F.expr("INTERVAL 1 DAY")).alias("begin_dt"), (F.col("final.begin_dt") - F.expr("INTERVAL 1 DAY")).alias("end_dt"))

    gaps_df = gap_at_start.unionByName(gap_at_end).unionByName(gaps_in_middle)
    return gaps_df


def find_unmatched_scopes(final_periods_df: DataFrame, original_scope_df: DataFrame) -> DataFrame:
    """Finds rows in the scope without any property."""
    scope_pk_cols = ["item_id", "location_id"]
    matched_scopes = final_periods_df.select(*scope_pk_cols).distinct()
    unmatched_df = original_scope_df.join(matched_scopes, scope_pk_cols, "left_anti")
    return unmatched_df


def assemble_final_result(
        final_periods_df: DataFrame, gaps_df: DataFrame, unmatched_df: DataFrame,
        condition_df: DataFrame, unit_df: DataFrame, metric_id: int, p_root_calc_id: int
) -> DataFrame:
    """Assembles all previous scope parts into result dataframe."""
    scope_pk_cols = ["item_id", "location_id"]
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

    final_df = combined_df.join(condition_df, "property_id", "left") \
        .join(unit_df, F.col("unit_id") == unit_df.id, "left") \
        .select(
            *scope_pk_cols,
            F.col("begin_dt"), F.col("end_dt"),
            F.lit(p_root_calc_id).alias("root_calc_id"),
            F.col("metric_id").cast(LongType()),
            F.col("unit_id"), F.col("operator"), F.col("value"),
            F.col("value_data_type"), F.col("is_exception")
        ).orderBy(*scope_pk_cols, "begin_dt")
    return final_df


def main(current_metric_id):
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("INFO: Step 1: Spark session created.")

    p_root_calc_id = create_root_calc_id(spark, "reversal")
    print(f"INFO: Step 2: Root calculation id created: {p_root_calc_id}")

    dfs = extract_data_for_procedure(spark)
    print("INFO: Data extracted from tables.")

    intersections_df = find_intersections(
        item_location_period_norm_df=dfs["item_location_period_norm"],
        property_norm_df=dfs["property_norm"],
        metric_id=current_metric_id
    )
    print("INFO: Step 3 results: Found intersections.")

    final_periods_df = calculate_final_periods(intersections_df)
    print("INFO: Step 4 results: Final periods calculated using iterative subtraction.")

    gaps_df = calculate_gaps(final_periods_df, dfs["item_location_period"])
    print("INFO: Step 5 results: Found 'gaps' (periods without property).")

    unmatched_df = find_unmatched_scopes(final_periods_df, dfs["item_location_period"])
    print("INFO: Step 6 results: Found unmatched scopes.")

    final_result_df = assemble_final_result(
        final_periods_df=final_periods_df,
        gaps_df=gaps_df,
        unmatched_df=unmatched_df,
        condition_df=dfs["condition"],
        unit_df=dfs["unit"],
        metric_id=current_metric_id,
        p_root_calc_id=p_root_calc_id
    )

    print("INFO: Step 7: Writing final result_item_location_period to PostgreSQL...")
    write_jdbc_data(final_result_df, result_item_location_period)
    print("INFO: Process completed successfully.")

    spark.stop()


if __name__ == '__main__':
    metric_id_str = os.environ.get("METRIC_ID")
    if metric_id_str:
        metric_id = int(metric_id_str)
        main(metric_id)
    else:
        print("ERROR: METRIC_ID environment variable not set.")


