from pyspark import pipelines as dp
#We are modernizing our Imperative Pipeline to Declarative
#We are using Lakeflow Ingestion (Auto Loader)
@dp.table(name="retail.logistics.bronze_shipment_customer")
def bronze_shipment_load():
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode","addNewColumns")
            .load("/Volumes/retail/logistics/datalake/logistics"))
            

@dp.table(name="retail.logistics.bronze_geotag")
def bronze_geotag_load():
    return(spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").option("inferColumnTypes","True").load("/Volumes/retail/logistics/datalake/geotag/"))


@dp.table(name="retail.logistics.bronze_shipment_order")
def bronze_shipment_order_load():
    return( spark.readStream.format("cloudFiles").option("cloudFiles.format","json").option("inferSchema",'True').option("multiline","True").load("/Volumes/retail/logistics/datalake/shipment_order/").select(
                "shipment_id",
                "order_id",
                "source_city",
                "destination_city",
                "shipment_status",
                "cargo_type",
                "vehicle_type",
                "payment_mode",
                "shipment_weight_kg",
                "shipment_cost",
                "shipment_date"
            )
    )

