from pyspark import pipelines as dp

dbutils.widgets.text("catalog","retail")
dbutils.widgets.text("schema","schema_retail")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

srcLocation = "dbfs:///Volumes/retail/schema_retail/volume_retail/"

@dp.table(name=f"{catalog}.{schema}.bronze_products")
def load_bronze_products():
    return (spark.readStream.
            format("cloudFiles").
            option("CloudFiles.format","csv").
            option("header","true").
            option("cloudFiles.inferSchema","true").
            option("cloudFiles.schemaLocation",f"{srcLocation}/products/_schemaLocation/").
            load(f"{srcLocation}/products"))
    

@dp.table(name=f"{catalog}.{schema}.bronze_customer")
def load_bronze_customer():
    return(
        spark.readStream.format("cloudFiles").
        option("cloudFiles.format","csv").
        option("cloudFiles.schemaLocation",f"{srcLocation}/customer/_schemaLocation/").
        option("cloudFiles.inferSchema","true").
        option("header","true").
        load(f"{srcLocation}/customer")
    )

@dp.table(name=f"{catalog}.{schema}.bronze_purchase")
def load_bronze_purchase():
    return(spark.
           readStream.
           format("cloudFiles").
           option("cloudFiles.format","csv").
           option("header","true").
           option("cloudFiles.schemaLocation",f"{srcLocation}/purchase/_schemaLocation/").
           option("cloudFiles.inferSchema","true").
           load(f"{srcLocation}/purchase")
        )
