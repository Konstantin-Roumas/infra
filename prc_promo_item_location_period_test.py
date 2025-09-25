from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import os
import logging
from jdbs_db_utils import write_jdbc_data, read_jdbc_data
from spark_utils import create_spark_session
import reversal_processor as rp

logger = logging.getLogger(__name__)

# --- Specific Configuration ---
SCOPE_PK = ["item_id", "location_id", "promo_id"]
RESULT_TABLE = 'result_promo_item_location_period_spark'
NORM_SCOPE_TABLE = "promo_item_location_period_norm"
ORIGINAL_SCOPE_TABLE = "promo_item_location_period"

def extract_data_for_procedure(spark: SparkSession) -> dict:
    """Extracts data from necessary SQL tables for this specific scope."""
    dfs = {
        "property_norm": read_jdbc_data("property_norm", spark),
        "metric_cluster_link": read_jdbc_data("metric_cluster_link", spark),
        NORM_SCOPE_TABLE: read_jdbc_data(NORM_SCOPE_TABLE, spark),
        ORIGINAL_SCOPE_TABLE: read_jdbc_data(ORIGINAL_SCOPE_TABLE, spark),
        "condition": read_jdbc_data("condition", spark),
        "unit": read_jdbc_data("unit", spark)
    }
    return dfs

def find_intersections(
        promo_item_location_period_norm_df: DataFrame,
        property_norm_df: DataFrame,
        mcl_df: DataFrame,
        metric_id: int
) -> DataFrame:
    """Finds all valid intersections between scope data and properties (+metric_cluster_link)."""
    scope = promo_item_location_period_norm_df.alias("scope")
    prop = property_norm_df.alias("prop")
    mcl = mcl_df.alias("mcl")

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
                F.coalesce(F.col(f"prop.{col_name}"), F.col(f"scope.{col_name}"), F.lit(0)) ==
                F.coalesce(F.col(f"scope.{col_name}"), F.lit(0))
        )

    intersections_df = scope.join(prop, join_condition, "inner") \
        .join(
            mcl,
            (F.col("prop.property_id") == F.col("mcl.property_id")) &
            (F.col("prop.metric_id") == F.col("mcl.metric_id")),
            "left"
        ) \
        .select(
        F.col("scope.item_id"),
        F.col("scope.location_id"),
        F.col("scope.promo_id"),
        F.greatest(F.col("prop.start_date"), F.col("scope.begin_dt")).alias("begin_dt"),
        F.least(F.col("prop.end_date"), F.col("scope.end_dt")).alias("end_dt"),
        F.col("prop.property_id"),
        F.col("prop.is_exception"),
        F.col("prop.order_number"),
        F.col("mcl.order_number").alias("mcl_order_number")
    )

    # Жёсткая проверка полноты MCL
    if intersections_df.filter(F.col("mcl_order_number").isNull()).head(1):
        raise ValueError("Отсутствует metric_cluster_link.order_number для части свойств — заполните приоритеты в metric_cluster_link.")

    return intersections_df

# --- Main Orchestrator ---
def main(current_metric_id: int):
    """Main processing pipeline for the promo_item_location_period scope."""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("INFO: Step 1: Spark session created.")

    p_root_calc_id = rp.create_root_calc_id(spark, "reversal")
    print(f"INFO: Step 2: Root calculation id created: {p_root_calc_id}")

    dfs = extract_data_for_procedure(spark)
    
    intersections_df = find_intersections(
        promo_item_location_period_norm_df=dfs[NORM_SCOPE_TABLE],
        property_norm_df=dfs["property_norm"],
        mcl_df=dfs["metric_cluster_link"],
        metric_id=current_metric_id
    )
    print("INFO: Step 3: Found intersections (with metric_cluster_link).")

    final_periods_df = rp.calculate_final_periods(intersections_df, SCOPE_PK)
    print("INFO: Step 4: Calculated final periods after slicing and gluing.")

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

if __name__ == "__main__":
    metric_id_env = os.environ.get("METRIC_ID")
    if metric_id_env:
        main(int(metric_id_env))
    else:
        print("ERROR: METRIC_ID environment variable not set.")
