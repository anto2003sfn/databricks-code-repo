from pyspark import pipelines as dp
from pyspark.sql.functions import *
#Applying the UDF in our pipeline
@dp.table(
    name="retail.logistics.silver_geotag_dlt",
    comment="Standardized geotag data",#This is for governance
    table_properties={"quality": "silver"}#This is for governance
)
@dp.expect("valid_latitude","latitude> -90 and latitude<90"
)
def silver_geotag_dlt():
    return (spark.readStream.table("retail.logistics.bronze_geotag").select(
        col("country"),col("city_name"),col("latitude"),col("longitude")))

@dp.table(name="retail.logistics.silver_shipment_customer",
              comment="Standard shipment data",
              table_properties={"quality":"silver"})
def silver_shipment_customer_dlt():
    return (spark.readStream.table("retail.logistics.bronze_shipment_customer").
            select(col("shipment_id").cast("bigint").alias("shipment_id"), col("age"),lower(col("role")).alias("role"), 
                   initcap(col("hub_location")).alias("origin_hub_city"),
                   concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"), initcap(col("hub_location")).alias("hub_location"),current_timestamp().alias("load_ts")))
    
@dp.table(name="retail.logistics.silver_shipment_order",
              comment="Standard shipment order data",
              table_properties={"quality":"silver"})
def silver_shipment_order_dlt():
    ship_date_col = to_date(col("shipment_date"), "yy-MM-dd") 
    return(spark.readStream.table("retail.logistics.bronze_shipment_order").withColumn("domain",lit("Logistics")).withColumn("load_ts",current_timestamp()).withColumn("is_expedited_flag_initial",lit(False).cast("boolean")).withColumn("shipment_date_clean",ship_date_col).withColumn("shipment_cost_clean",round(col("shipment_cost"), 2)).withColumn("shipment_weight_clean", col("shipment_weight_kg").cast("double")).withColumn("route_segment", concat_ws("-", col("source_city"), col("destination_city"))).withColumn("vehicle_identifier", concat_ws("_", col("vehicle_type"), col("shipment_id"))).withColumn("shipment_year", year(ship_date_col)).withColumn("shipment_month", month(ship_date_col)).withColumn("is_weekend", when(dayofweek(ship_date_col).isin([1, 7]), True).otherwise(False)
        ).withColumn("is_expedited", when(col("shipment_status").isin(["IN_TRANSIT", "DELIVERED"]), True).otherwise(False)).withColumn("cost_per_kg", round(col("shipment_cost") / col("shipment_weight_kg"), 2))
        .withColumn("tax_amount", round(col("shipment_cost") * 0.18, 2))
        .withColumn("days_since_shipment", datediff(current_date(), ship_date_col))
        .withColumn("is_high_value", when(col("shipment_cost") > 50000, True).otherwise(False)).withColumn("order_prefix", substring(col("order_id"), 1, 3))
        .withColumn("order_sequence", substring(col("order_id"), 4, 10))
        .withColumn("ship_day", dayofmonth(ship_date_col))
        .withColumn("route_lane", concat_ws("->", col("source_city"), col("destination_city"))))

