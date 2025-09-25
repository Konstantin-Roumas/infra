from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.types import LongType, IntegerType
import pyspark.sql.functions as F
from jdbs_db_utils import read_jdbc_data

def create_root_calc_id(spark: SparkSession, table_name: str) -> int:
    """Creates a new unique id for the reversal calculation run."""
    root_calc = read_jdbc_data(spark=spark, table_name=table_name)
    max_root_calc_id = root_calc.agg(F.max("reversal_id")).collect()[0][0]
    return (max_root_calc_id or 0) + 1

def _ensure_mcl_order(df: DataFrame) -> DataFrame:
    from pyspark.sql.types import IntegerType
    if "mcl_order_number" not in df.columns:
        df = df.withColumn("mcl_order_number", F.lit(None).cast(IntegerType()))
    return df

def _effective_priority():
    return F.coalesce(F.col("mcl_order_number"), F.col("order_number"))

def calculate_final_periods(intersections_df: DataFrame, scope_pk_cols: list) -> DataFrame:
    """
    Универсальная реализация «событийных точек» с приоритетом:
    metric_cluster_link.order_number → property_norm.order_number.
    """
    df = _ensure_mcl_order(intersections_df)

    event_points_df = df.select(
        *scope_pk_cols, F.array(F.col("begin_dt"), F.col("end_dt") + F.expr("INTERVAL 1 DAY")).alias("dates")
    ).groupBy(*scope_pk_cols).agg(F.array_sort(F.flatten(F.collect_set("dates"))).alias("event_dates"))

    intervals_df = event_points_df.select(
        *scope_pk_cols, F.explode("event_dates").alias("interval_start")
    ).withColumn(
        "interval_end",
        (F.lead("interval_start", 1).over(Window.partitionBy(*scope_pk_cols).orderBy("interval_start")) - F.expr("INTERVAL 1 DAY")).cast("date")
    ).dropna(subset=["interval_end"])

    interval_props_df = intervals_df.join(df, scope_pk_cols).filter(
        (F.col("interval_start") <= F.col("end_dt")) & (F.col("interval_end") >= F.col("begin_dt"))
    )

    interval_props_with_eff = interval_props_df.withColumn("effective_priority", _effective_priority())
    conflict_df = interval_props_with_eff.groupBy(
        *scope_pk_cols, "interval_start", "is_exception", "effective_priority"
    ).agg(F.countDistinct("property_id").alias("prop_cnt")).filter(F.col("prop_cnt") > 1)
    if conflict_df.head(1):
        raise ValueError("Найдены противоречивые настройки: одинаковый приоритет у нескольких свойств в одном интервале.")

    win_spec = Window.partitionBy(*scope_pk_cols, "interval_start").orderBy(
        F.col("is_exception").desc(),
        _effective_priority().asc(),
        F.col("property_id").asc()
    )
    best_prop_df = interval_props_df.withColumn("rn", F.row_number().over(win_spec)).filter(F.col("rn") == 1)

    change_detector_win = Window.partitionBy(*scope_pk_cols).orderBy("interval_start")
    coalesce_df = best_prop_df.withColumn("prev_prop", F.lag("property_id", 1).over(change_detector_win)) \
       .withColumn("is_new_group", F.when(F.col("property_id")!= F.col("prev_prop"), 1).otherwise(0)) \
       .withColumn("group_id", F.sum("is_new_group").over(change_detector_win))

    final_periods_df = coalesce_df.groupBy(*scope_pk_cols, "group_id", "property_id", "is_exception") \
       .agg(F.min("interval_start").alias("begin_dt"), F.max("interval_end").alias("end_dt")) \
       .select(*scope_pk_cols, "begin_dt", "end_dt", "property_id", "is_exception") \
       .orderBy(*scope_pk_cols, "begin_dt")

    return final_periods_df

def calculate_gaps(final_periods_df: DataFrame, original_scope_df: DataFrame, scope_pk_cols: list) -> DataFrame:
    """Calculates 'gaps' - date periods without any property."""
    final_periods_aliased = final_periods_df.alias("final")
    original_scope_aliased = original_scope_df.alias("orig")

    lag_window = Window.partitionBy(*scope_pk_cols).orderBy(F.col("final.begin_dt"))

    processed_periods = final_periods_aliased.withColumn("prev_end_dt", F.lag("final.end_dt", 1).over(lag_window)) \
       .join(original_scope_aliased, scope_pk_cols) \
       .withColumn("min_begin_dt", F.min("final.begin_dt").over(Window.partitionBy(*scope_pk_cols))) \
       .withColumn("max_end_dt", F.max("final.end_dt").over(Window.partitionBy(*scope_pk_cols)))

    gap_at_start = processed_periods.filter(
        (F.col("final.begin_dt") == F.col("min_begin_dt")) & (F.col("orig.begin_dt") < F.col("final.begin_dt"))
    ).select(*scope_pk_cols, F.col("orig.begin_dt").alias("begin_dt"), (F.col("final.begin_dt") - F.expr("INTERVAL 1 DAY")).alias("end_dt"))

    gap_at_end = processed_periods.filter(
        (F.col("final.end_dt") == F.col("max_end_dt")) & (F.col("orig.end_dt") > F.col("final.end_dt"))
    ).select(*scope_pk_cols, (F.col("final.end_dt") + F.expr("INTERVAL 1 DAY")).alias("begin_dt"), F.col("orig.end_dt").alias("end_dt"))

    gaps_in_middle = processed_periods.filter(
        F.col("prev_end_dt").isNotNull() & (F.col("final.begin_dt") - F.expr("INTERVAL 1 DAY") > F.col("prev_end_dt"))
    ).select(*scope_pk_cols, (F.col("prev_end_dt") + F.expr("INTERVAL 1 DAY")).alias("begin_dt"), (F.col("final.begin_dt") - F.expr("INTERVAL 1 DAY")).alias("end_dt"))

    gaps_df = gap_at_start.unionByName(gap_at_end).unionByName(gaps_in_middle)
    return gaps_df

def find_unmatched_scopes(final_periods_df: DataFrame, original_scope_df: DataFrame, scope_pk_cols: list) -> DataFrame:
    """Finds rows in the scope without any property."""
    matched_scopes = final_periods_df.select(*scope_pk_cols).distinct()
    unmatched_df = original_scope_df.join(matched_scopes, scope_pk_cols, "left_anti")
    return unmatched_df

def assemble_final_result(
        final_periods_df: DataFrame,
        gaps_df: DataFrame,
        unmatched_df: DataFrame,
        condition_df: DataFrame,
        unit_df: DataFrame,
        metric_id: int,
        p_root_calc_id: int,
        scope_pk_cols: list
) -> DataFrame:
    """Assembles all previous scope parts into result dataframe, joining condition and unit."""
    periods_with_props = final_periods_df.withColumn("metric_id", F.lit(metric_id))

    gaps_prepared_df = gaps_df.withColumn("property_id", F.lit(None).cast(LongType())) \
       .withColumn("is_exception", F.lit(False)) \
       .withColumn("metric_id", F.lit(metric_id))

    unmatched_prepared_df = unmatched_df.withColumn("property_id", F.lit(None).cast(LongType())) \
       .withColumn("is_exception", F.lit(False)) \
       .withColumn("metric_id", F.lit(metric_id))

    combined_df = periods_with_props.unionByName(gaps_prepared_df).unionByName(unmatched_prepared_df)

    final_df = combined_df.join(condition_df, "property_id", "left") \
       .join(unit_df, F.col("unit_id") == unit_df.id, "left") \
       .select(
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
