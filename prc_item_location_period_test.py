from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.types import LongType, IntegerType, DateType, ArrayType, StructType, StructField
import pyspark.sql.functions as F
import os
import logging
from jdbs_db_utils import write_jdbc_data, read_jdbc_data
from spark_utils import create_spark_session
from datetime import timedelta
import reversal_processor as rp

logger = logging.getLogger(__name__)

# --- Specific Configuration ---
SCOPE_PK = ["item_id", "location_id"]
RESULT_TABLE = 'result_item_location_period'
NORM_SCOPE_TABLE = "item_location_period_norm"
ORIGINAL_SCOPE_TABLE = "item_location_period"

def extract_data_for_procedure(spark: SparkSession) -> dict:
    """Extracts data from necessary SQL tables for this specific scope."""
    dfs = {
        "property_norm": read_jdbc_data("property_norm", spark),
        NORM_SCOPE_TABLE: read_jdbc_data(NORM_SCOPE_TABLE, spark),
        ORIGINAL_SCOPE_TABLE: read_jdbc_data(ORIGINAL_SCOPE_TABLE, spark),
        "condition": read_jdbc_data("condition", spark),
        "unit": read_jdbc_data("unit", spark)
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

def calculate_final_periods(intersections_df: DataFrame) -> DataFrame:
    """
    Calculates final periods using the "Iterative Subtraction" algorithm to match the
    PostgreSQL fnc_get_period logic. It subtracts date ranges of higher-priority
    properties from lower-priority ones. This logic is SPECIFIC to this scope.
    """
    scope_pk_cols = ["item_id", "location_id"]

    def subtract_periods_udf_logic(current_start, current_end, blocker_starts, blocker_ends):
        if current_start > current_end:
            return []
            
        if blocker_starts is None or len(blocker_starts) == 0:
            return [(current_start, current_end)]

        blockers = sorted(zip(blocker_starts, blocker_ends))
        
        available_periods = [(current_start, current_end)]

        for b_start, b_end in blockers:
            if b_start > b_end:
                continue

            next_available_periods = []
            for a_start, a_end in available_periods:
                if b_end < a_start or b_start > a_end:
                    next_available_periods.append((a_start, a_end))
                    continue
                
                if a_start < b_start:
                    new_end = b_start - timedelta(days=1)
                    if a_start <= new_end:
                        next_available_periods.append((a_start, new_end))
                
                if a_end > b_end:
                    new_start = b_end + timedelta(days=1)
                    if new_start <= a_end:
                        next_available_periods.append((new_start, a_end))
            
            available_periods = next_available_periods
            if not available_periods:
                break
        return available_periods

    udf_return_schema = ArrayType(StructType())

    subtract_periods_udf = F.udf(subtract_periods_udf_logic, udf_return_schema)

    priority_window = Window.partitionBy(*scope_pk_cols) \
      .orderBy(F.col("is_exception").desc(), F.col("order_number").asc(), F.col("property_id").asc()) \
      .rowsBetween(Window.unboundedPreceding, Window.currentRow - 1)

    periods_with_blockers = intersections_df.withColumn(
        "blocker_starts", F.collect_list("begin_dt").over(priority_window)
    ).withColumn(
        "blocker_ends", F.collect_list("end_dt").over(priority_window)
    )

    result_with_period_arrays = periods_with_blockers.withColumn(
        "final_periods",
        subtract_periods_udf(F.col("begin_dt"), F.col("end_dt"), F.col("blocker_starts"), F.col("blocker_ends"))
    )

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

def main(current_metric_id: int):
    """Main processing pipeline for the item_location_period scope."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("INFO: Step 1: Spark session created.")

    p_root_calc_id = rp.create_root_calc_id(spark, "reversal")
    print(f"INFO: Step 2: Root calculation id created: {p_root_calc_id}")

    dfs = extract_data_for_procedure(spark)
    print("INFO: Data extracted from tables.")

    intersections_df = find_intersections(
        item_location_period_norm_df=dfs[NORM_SCOPE_TABLE],
        property_norm_df=dfs["property_norm"],
        metric_id=current_metric_id
    )
    print("INFO: Step 3: Found intersections.")

    final_periods_df = calculate_final_periods(intersections_df)
    print("INFO: Step 4: Final periods calculated using iterative subtraction.")

    gaps_df = rp.calculate_gaps(final_periods_df, dfs[ORIGINAL_SCOPE_TABLE], SCOPE_PK)
    print("INFO: Step 5: Found 'gaps' (periods without property).")

    unmatched_df = rp.find_unmatched_scopes(final_periods_df, dfs[ORIGINAL_SCOPE_TABLE], SCOPE_PK)
    print("INFO: Step 6: Found unmatched scopes.")

    final_result_df = rp.assemble_final_result(
        final_periods_df=final_periods_df,
        gaps_df=gaps_df,
        unmatched_df=unmatched_df,
        condition_df=dfs["condition"],
        unit_df=dfs["unit"],
        metric_id=current_metric_id,
        p_root_calc_id=p_root_calc_id,
        scope_pk_cols=SCOPE_PK
    )

    print(f"INFO: Step 7: Writing final result to {RESULT_TABLE}...")
    write_jdbc_data(final_result_df, RESULT_TABLE)
    print("INFO: Process completed successfully.")
    spark.stop()

if __name__ == '__main__':
    metric_id_str = os.environ.get("METRIC_ID")
    if metric_id_str:
        main(int(metric_id_str))
    else:
        print("ERROR: METRIC_ID environment variable not set.")
