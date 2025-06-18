import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, expr, coalesce

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Daily Weather table
@dlt.table(name="bronze.daily_weather")
def daily_weather_bronze():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .load("/Volumes/harrison_chen_catalog/synthetic_energy/energy_volume/Gas_Emissions/daily_weather/")
    )

@dlt.table(name="silver.daily_weather")
@dlt.expect("valid_temperature_celsius", "temperature_celsius BETWEEN -40 AND 50")
@dlt.expect("valid_humidity_percentage", "humidity_percentage BETWEEN 0 AND 100")
@dlt.expect_or_drop("valid_date_range", "date >= '2020-01-01'")
def daily_weather_silver():
    return spark.readStream.table("bronze.daily_weather")

# Sensor Emissions table
@dlt.table(name="bronze.sensor_emissions")
def sensor_emissions_bronze():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .load("/Volumes/harrison_chen_catalog/synthetic_energy/energy_volume/Gas_Emissions/sensor_emissions/")
    )

@dlt.table(name="silver.sensor_emissions")
@dlt.expect("valid_methane_level", "methane_level BETWEEN 0 AND 1000")
@dlt.expect("valid_co2_level", "co2_level BETWEEN 0 AND 1000")
@dlt.expect("valid_nox_level", "nox_level BETWEEN 0 AND 1000")
@dlt.expect("valid_temperature", "temperature BETWEEN -20 AND 100")
@dlt.expect("valid_pressure", "pressure BETWEEN 0 AND 1000")
@dlt.expect("valid_flow_rate", "flow_rate BETWEEN 0 AND 10000")
@dlt.expect_or_drop("valid_timestamp_range", "timestamp >= '2020-01-01'")
def sensor_emissions_silver():
    return spark.readStream.table("bronze.sensor_emissions")

# Change Feed Tables
@dlt.table(name="bronze.valve_compliance_changes")
def valve_compliance_changes_bronze():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .load("/Volumes/harrison_chen_catalog/synthetic_energy/energy_volume/Gas_Emissions/valve_compliance_changes/")
    )

@dlt.create_streaming_table(name="silver.valve_compliance_changes")
def valve_compliance_changes_silver():
    pass

@dlt.apply_changes(
    target="silver.valve_compliance_changes",
    source="bronze.valve_compliance_changes",
    keys=["valve_id", "asset_id"],
    sequence_by="change_timestamp",
    stored_as_scd_type=2  # Type 2 SCD to maintain history
)

@dlt.table(name="bronze.asset_config_changes")
def asset_config_changes_bronze():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .load("/Volumes/harrison_chen_catalog/synthetic_energy/energy_volume/Gas_Emissions/asset_config_changes/")
    )

@dlt.create_streaming_table(name="silver.asset_config_changes")
def asset_config_changes_silver():
    pass

@dlt.apply_changes(
    target="silver.asset_config_changes",
    source="bronze.asset_config_changes",
    keys=["config_id", "asset_id"],
    sequence_by="change_timestamp",
    stored_as_scd_type=2  # Type 2 SCD to maintain history
)

@dlt.table(name="bronze.maintenance_record")
def maintenance_record_bronze():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .load("/Volumes/harrison_chen_catalog/synthetic_energy/energy_volume/Gas_Emissions/maintenance_record/")
    )

@dlt.table(name="silver.maintenance_record")
@dlt.expect("valid_cost", "cost BETWEEN 0 AND 100000")
@dlt.expect_or_drop("valid_date_range", "start_time >= '2020-01-01'")
def maintenance_record_silver():
    return spark.readStream.table("bronze.maintenance_record")

# Gold table for valve compliance history
@dlt.table(name="gold.valve_compliance_history")
def valve_compliance_history_gold():
    # Get current and historical valve compliance states
    return (spark.readStream.table("silver.valve_compliance_changes")
            # Filter out NULL records in key fields
            .filter("valve_id IS NOT NULL AND asset_id IS NOT NULL")
            # Filter out records with NULL compliance status
            .filter("compliance_status IS NOT NULL")
            # Select and rename columns
            .select(
                "valve_id",
                "asset_id",
                "compliance_status",
                "inspector_id",
                "inspection_notes",
                "change_timestamp",
                "__START_AT",
                "__END_AT"
            )
            .withColumnRenamed("__START_AT", "valid_from")
            .withColumnRenamed("__END_AT", "valid_to")
            # Add business insights
            .withColumn("compliance_duration_days", 
                       expr("datediff(valid_to, valid_from)"))
            .withColumn("is_current_record", 
                       expr("CASE WHEN valid_to IS NULL THEN 'True' ELSE 'False' END"))
            # Deduplicate based on valve_id, asset_id, and valid_from
            .dropDuplicates(["valve_id", "asset_id", "valid_from"])
    )

# Gold table for asset risk assessment
@dlt.table(name="gold.asset_risk_assessment")
def asset_risk_assessment_gold():
    # Get the compliance history
    compliance_history = spark.readStream.table("gold.valve_compliance_history")
    
    # Get recent emissions data
    emissions = spark.readStream.table("silver.sensor_emissions").filter("timestamp >= '2020-01-01'")
    
    # Combine compliance history with emissions data for risk assessment
    return (compliance_history
            .join(emissions,
                  (compliance_history.asset_id == emissions.asset_id) &
                  (emissions.timestamp >= compliance_history.valid_from) &
                  (emissions.timestamp < compliance_history.valid_to),
                  "left")
            .groupBy(
                compliance_history.asset_id,
                compliance_history.valve_id,
                compliance_history.compliance_status,
                compliance_history.valid_from,
                compliance_history.valid_to
            )
            .agg({
                "methane_level": "avg",
                "co2_level": "avg",
                "nox_level": "avg"
            })
            .select(
                "asset_id",
                "valve_id",
                "compliance_status",
                "valid_from",
                "valid_to",
                dlt.col("avg(methane_level)").alias("avg_methane_level"),
                dlt.col("avg(co2_level)").alias("avg_co2_level"),
                dlt.col("avg(nox_level)").alias("avg_nox_level")
            )) 